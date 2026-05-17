use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, ready};
use std::time::Duration;
use std::{path::PathBuf, process::ExitCode};

use anyhow::{Context, anyhow, bail};
use clap::Parser;
use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, FileAttr, FileType,
    FsResult, Inode, MountOptions, ReplyAttr, ReplyCreate, ReplyEntry, ReplyOpen, ReplyReadlink,
    ReplyStatfs, Request, Timestamp,
};
use futures_util::AsyncRead;
use futures_util::io::AsyncReadExt;
use jj_lib::backend::{BackendResult, CopyId, Tree, TreeId, TreeValue};
use jj_lib::commit::Commit;
use jj_lib::merged_tree::MergedTree;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo, StoreFactories};
use jj_lib::repo_path::{RepoPathBuf, RepoPathComponent, RepoPathComponentBuf};
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use libc::O_TRUNC;
use pin_project_lite::pin_project;
use rustc_hash::FxHashMap;
use slab::Slab;
use tokio::sync::RwLock;
use tracing::{error, trace};

#[derive(Parser)]
#[command(version)]
struct Args {
    repository: PathBuf,
    mountpoint: PathBuf,
}

fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    if let Err(e) = run() {
        error!("{:#}", e);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let rt = tokio::runtime::Runtime::new().context("initializing tokio")?;
    trace!("opening repository");
    let fs = rt
        .block_on(Fs::new(&args.repository))
        .context("opening repository")?;
    let shared = fs.shared.clone();
    trace!("mounting");
    let result = fractal_fuse::Session::new(args.mountpoint, MountOptions::new().fs_name("jj"))
        .context("mounting")?
        .queue_depth(128)
        .run(fs);
    rt.block_on(shared.write_commit())
        .context("committing final state")?;
    if let Err(e) = result {
        if e.kind() == io::ErrorKind::Unsupported {
            bail!(
                "initializing FUSE: {e:#} -- try `echo Y | sudo tee /sys/module/fuse/parameters/enable_uring`"
            );
        }
        return Err(e).context("initializing FUSE");
    }
    Ok(())
}

struct Fs {
    shared: Arc<Shared>,
}

impl Fs {
    async fn new(path: &Path) -> anyhow::Result<Self> {
        let path = path.join(".jj/repo");
        let mut cfg = jj_lib::config::StackedConfig::with_defaults();
        cfg.load_dir(jj_lib::config::ConfigSource::Repo, &path)?;
        let settings = UserSettings::from_config(cfg)?;
        let loader = jj_lib::repo::RepoLoader::init_from_file_system(
            &settings,
            &path,
            &StoreFactories::default(),
        )?;
        let repo = loader.load_at_head().await?;
        let commit_id = repo
            .view()
            .get_wc_commit_id(&WorkspaceName::DEFAULT)
            .ok_or_else(|| anyhow!("no default workspace"))?;
        let commit = repo
            .store()
            .get_commit_async(commit_id)
            .await
            .context("reading commit")?;
        let Some(tree_id) = commit.tree_ids().as_resolved().cloned() else {
            bail!("conflicted trees are not implemented");
        };
        Ok(Self {
            shared: Arc::new(Shared {
                repo_state: RwLock::new(RepoState { repo, commit }),
                inodes: InodeTable::new(tree_id),
            }),
        })
    }
}

impl fractal_fuse::Filesystem for Fs {
    async fn destroy(&self) {
        if let Err(e) = self.shared.write_commit().await {
            error!("failed to flush on exit: {:#}", e);
        }
    }

    async fn lookup(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<ReplyEntry> {
        let Ok(name) = str::from_utf8(name.as_bytes()) else {
            return Err(ENOENT);
        };
        let Ok(name) = RepoPathComponent::new(name) else {
            return Err(ENOENT);
        };
        let repo = self.shared.repo().await;
        let inode = self
            .shared
            .inodes
            .get_or_insert_and_ref(&repo, parent, name)
            .await?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: self.shared.inodes.stat(&repo, inode).await?,
            generation: 0,
        })
    }

    fn forget(&self, _: Request, inode: Inode, nlookup: u64) {
        let shared = self.shared.clone();
        compio_runtime::spawn(async move {
            shared.inodes.forget(inode, nlookup).await;
        })
        .detach();
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: Inode,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        Ok(ReplyAttr {
            ttl: TTL,
            attr: self
                .shared
                .inodes
                .stat(&*self.shared.repo().await, inode)
                .await?,
        })
    }

    async fn open(&self, _req: Request, inode: Inode, flags: u32) -> FsResult<ReplyOpen> {
        if flags & O_TRUNC as u32 != 0 {
            if let Err(e) = self
                .shared
                .inodes
                .trunc(self.shared.repo().await.store(), inode, 0)
                .await
            {
                let path = self.shared.inodes.get_path(inode).await;
                error!("truncating {path:?} for open: {e:#}");
                return Err(EIO);
            }
        }
        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
            backing_id: 0,
        })
    }

    async fn create(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> FsResult<ReplyCreate> {
        let name = translate_name(name)?;
        let repo = self.shared.repo().await;
        let path = self.shared.inodes.get_path(parent).await.join(name);
        let id = repo
            .store()
            .write_file(&path, &mut &[][..])
            .await
            .map_err(|e| {
                error!("creating empty file at {path:?}: {e:#}");
                EIO
            })?;
        let value = TreeValue::File {
            id,
            executable: mode & 0o100 != 0,
            copy_id: CopyId::placeholder(), // ???
        };
        let attr = value_stat(&value);
        let ino = self
            .shared
            .inodes
            .insert(repo.store(), parent, name, value)
            .await
            .map_err(|e| {
                error!("updating directory metadata leading file: {e:#}");
                EIO
            })?;
        Ok(ReplyCreate {
            ttl: TTL,
            attr: FileAttr { ino, ..attr },
            generation: 0,
            fh: 0,
            flags: 0,
        })
    }

    async fn release(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        _flock_release: bool,
    ) -> FsResult<()> {
        if let Err(e) = self.shared.write_commit().await {
            error!("writing commit on close: {:#}", e);
        }
        Ok(())
    }

    async fn read(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        self.shared
            .inodes
            .read(&*self.shared.repo().await, inode, offset, buf)
            .await
    }

    async fn write(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> FsResult<usize> {
        let inodes = self.shared.inodes.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        let mut mutable_state = state.mutable_state.write().await;
        let TreeValue::File {
            id,
            executable,
            copy_id,
        } = &mutable_state.value
        else {
            return Err(EISDIR);
        };
        let executable = *executable;
        let copy_id = copy_id.clone();
        let repo = self.shared.repo().await;
        let file = repo.store().read_file(&state.path, id).await.map_err(|e| {
            error!("opening {:?}: {:#}", state.path, e);
            EIO
        })?;
        trace!("opened old file {id}");
        // Create a new file with the specified data overwritten
        let updated_file = repo
            .store()
            .write_file(
                &state.path,
                &mut Overwrite {
                    inner: file,
                    offset,
                    data,
                },
            )
            .await
            .map_err(|e| {
                error!("writing {:?}: {:#}", state.path, e);
                EIO
            })?;
        trace!("created edited file {updated_file}");
        mutable_state.value = TreeValue::File {
            id: updated_file,
            executable,
            copy_id,
        };
        drop(mutable_state);

        self.shared
            .inodes
            .update_trees(repo.store(), inode)
            .await
            .map_err(|e| {
                error!(
                    "updating directory metadata leading to written file {:?}: {e:#}",
                    state.path
                );
                EIO
            })?;

        Ok(data.len())
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> FsResult<ReplyEntry> {
        let parent_path = self.shared.inodes.get_path(parent).await;
        let name = translate_name(name)?;
        let path = parent_path.join(name);
        let repo = self.shared.repo().await;
        let id = repo
            .store()
            .write_symlink(
                &path,
                link.to_str().ok_or_else(|| {
                    error!("non-Unicode symlink target: {link:?}");
                    EINVAL
                })?,
            )
            .await
            .map_err(|e| {
                error!("writing symlink {path:?}: {:#}", e);
                EIO
            })?;
        let value = TreeValue::Symlink(id);
        let attr = value_stat(&value);
        let ino = self
            .shared
            .inodes
            .insert(repo.store(), parent, name, value)
            .await
            .map_err(|e| {
                error!("updating directory metadata leading to symlink {path:?}: {e:#}");
                EIO
            })?;

        self.shared.write_commit().await.map_err(|e| {
            error!("committing new symlink: {e:#}");
            EIO
        })?;

        Ok(ReplyEntry {
            ttl: TTL,
            attr: FileAttr { ino, ..attr },
            generation: 0,
        })
    }

    async fn readlink(&self, _req: Request, inode: Inode) -> FsResult<ReplyReadlink> {
        let (path, TreeValue::Symlink(id)) = self.shared.inodes.get_path_value(inode).await else {
            return Err(EINVAL);
        };
        let target = self
            .shared
            .repo()
            .await
            .store()
            .read_symlink(&path, &id)
            .await
            .map_err(|e| {
                error!("reading symlink {path:?}: {:#}", e);
                EIO
            })?;
        Ok(ReplyReadlink {
            data: target.into_bytes(),
        })
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        self.shared
            .inodes
            .readdir(
                &*self.shared.repo().await,
                inode,
                offset,
                size,
                |value, name, offset| DirectoryEntry {
                    ino: 0,
                    offset,
                    kind: value_ty(value),
                    name: name.as_bytes().to_vec(),
                },
            )
            .await
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        self.shared
            .inodes
            .readdir(
                &*self.shared.repo().await,
                inode,
                offset,
                size,
                |value, name, offset| DirectoryEntryPlus {
                    ino: 0,
                    offset,
                    kind: value_ty(value),
                    name: name.as_bytes().to_vec(),
                    entry_ttl: TTL,
                    attr: value_stat(value),
                    generation: 0,
                },
            )
            .await
    }

    async fn opendir(&self, _req: Request, _inode: u64, _flags: u32) -> FsResult<ReplyOpen> {
        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
            backing_id: 0,
        })
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> FsResult<ReplyEntry> {
        let name = translate_name(name)?;
        let repo = self.shared.repo().await;
        let value = TreeValue::Tree(repo.store().empty_tree_id().clone());
        let attr = value_stat(&value);
        let ino = self
            .shared
            .inodes
            .insert(repo.store(), parent, name, value)
            .await
            .map_err(|e| {
                error!("updating directory metadata leading to file: {e:#}");
                EIO
            })?;

        self.shared.write_commit().await.map_err(|e| {
            error!("committing new directory: {e:#}");
            EIO
        })?;

        Ok(ReplyEntry {
            ttl: TTL,
            attr: FileAttr { ino, ..attr },
            generation: 0,
        })
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> FsResult<ReplyStatfs> {
        Ok(ReplyStatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 2,
            ffree: 0,
            bsize: 512,
            namelen: 255,
            frsize: 512,
        })
    }

    async fn access(&self, _req: Request, _inode: u64, _mask: u32) -> FsResult<()> {
        Ok(())
    }
}

struct Shared {
    repo_state: RwLock<RepoState>,
    inodes: InodeTable,
}

impl Shared {
    async fn repo(&self) -> Arc<ReadonlyRepo> {
        self.repo_state.read().await.repo.clone()
    }

    async fn write_commit(&self) -> anyhow::Result<()> {
        let TreeValue::Tree(id) = self.inodes.get(FUSE_ROOT_ID).await else {
            unreachable!()
        };
        // Rewrite the commit with the current tree
        let mut state = self.repo_state.write().await;
        let mut tx = state.repo.start_transaction();
        let new_commit = tx
            .repo_mut()
            .rewrite_commit(&state.commit)
            .set_tree(MergedTree::resolved(state.repo.store().clone(), id))
            .write()
            .await
            .context("rewriting commit")?;
        state.commit = new_commit;
        trace!("updated current commit to {}", state.commit.id());
        tx.repo_mut()
            .rebase_descendants()
            .await
            .context("rebasing descendants")?;
        state.repo = tx
            .commit("jj-fuse flush")
            .await
            .context("committing transaction")?;
        Ok(())
    }
}

/// Identifies the committed state a VFS view is based on
struct RepoState {
    repo: Arc<ReadonlyRepo>,
    commit: Commit,
}

struct InodeTable {
    inodes: RwLock<Slab<InodeState>>,
}

impl InodeTable {
    fn new(root_id: TreeId) -> Self {
        let root_inode = InodeState::new(RepoPathBuf::root(), TreeValue::Tree(root_id.clone()));
        Self {
            inodes: RwLock::new(Slab::from_iter([
                // Dummy inode to reserve slot 0
                (
                    0,
                    InodeState::new(RepoPathBuf::root(), TreeValue::Tree(root_id)),
                ),
                (FUSE_ROOT_ID as usize, root_inode),
            ])),
        }
    }

    async fn get(&self, inode: Inode) -> TreeValue {
        self.inodes
            .read()
            .await
            .get(inode as usize)
            .unwrap()
            .mutable_state
            .read()
            .await
            .value
            .clone()
    }

    async fn get_path(&self, inode: Inode) -> RepoPathBuf {
        self.inodes
            .read()
            .await
            .get(inode as usize)
            .unwrap()
            .path
            .clone()
    }

    async fn get_path_value(&self, inode: Inode) -> (RepoPathBuf, TreeValue) {
        let inodes = self.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        (
            state.path.clone(),
            state.mutable_state.read().await.value.clone(),
        )
    }

    async fn read(
        &self,
        repo: &ReadonlyRepo,
        inode: Inode,
        mut offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        let (path, TreeValue::File { id, .. }) = self.get_path_value(inode).await else {
            return Err(EISDIR);
        };
        let mut file = repo.store().read_file(&path, &id).await.map_err(|e| {
            error!("opening {path:?}: {:#}", e);
            EIO
        })?;
        // Brute-force seek
        let mut scratch = [0; 4096];
        while offset > 0 {
            let n = Ord::min(offset, scratch.len() as u64) as usize;
            offset -= file.read(&mut scratch[..n]).await.map_err(|e| {
                error!("reading {path:?}: {:#}", e);
                EIO
            })? as u64;
        }
        let n = file.read(buf).await.map_err(|e| {
            error!("reading {path:?}: {:#}", e);
            EIO
        })?;
        Ok(n)
    }

    async fn stat(&self, repo: &ReadonlyRepo, inode: Inode) -> FsResult<FileAttr> {
        let (path, value) = self.get_path_value(inode).await;
        let mut size = 0;
        if let TreeValue::File { id, .. } = &value {
            let meta = repo
                .store()
                .get_file_metadata(&path, &id)
                .await
                .map_err(|e| {
                    error!("fetching metadata for {path:?}: {:#}", e);
                    EIO
                })?;
            size = meta.size;
        };
        Ok(FileAttr {
            ino: inode,
            size,
            ..value_stat(&value)
        })
    }

    async fn readdir<T, F>(
        &self,
        repo: &ReadonlyRepo,
        inode: Inode,
        offset: u64,
        size: u32,
        mut f: F,
    ) -> FsResult<Vec<T>>
    where
        F: FnMut(&TreeValue, &str, u64) -> T,
    {
        let mut entries = Vec::new();
        let dir_path;
        let tree_id;
        {
            let inodes = self.inodes.read().await;
            let state = inodes.get(inode as usize).unwrap();
            let value = state.mutable_state.read().await.value.clone();
            let TreeValue::Tree(id) = &value else {
                return Err(ENOTDIR);
            };
            dir_path = state.path.clone();
            tree_id = id.clone();
            if offset < 1 {
                entries.push(f(&value, ".", 1));
            }
            if offset < 2
                && let Some(p) = &state.parent
            {
                entries.push(f(
                    &inodes
                        .get(p.parent)
                        .unwrap()
                        .mutable_state
                        .read()
                        .await
                        .value,
                    "..",
                    2,
                ));
            }
        }
        let tree = repo
            .store()
            .get_tree(dir_path.clone(), &tree_id)
            .await
            .map_err(|e| {
                error!("opening directory {dir_path:?}: {:#}", e);
                EIO
            })?;
        let iter = tree.entries_non_recursive();
        if let Some(hint) = iter.size_hint().1 {
            entries.reserve_exact(hint);
        }
        let skip = offset.saturating_sub(entries.len() as u64) as usize;
        let base = offset + entries.len() as u64;
        let limit = size as usize - skip;
        for (i, entry) in iter.skip(skip).take(limit).enumerate() {
            let name = match entry.name().to_fs_name() {
                Ok(name) => name,
                Err(e) => {
                    error!("hiding unrepresentable name {:?}: {:#}", entry.name(), e);
                    continue;
                }
            };
            entries.push(f(entry.value(), name, i as u64 + base + 1));
        }
        Ok(entries)
    }

    /// Returns `None` if not already resident
    async fn get_and_ref(&self, parent: Inode, name: &RepoPathComponent) -> Option<Inode> {
        let inodes = self.inodes.read().await;
        let i = *inodes
            .get(parent as usize)?
            .mutable_state
            .read()
            .await
            .children
            .as_ref()?
            .get(name)?;
        inodes
            .get(i)
            .unwrap()
            .references
            .fetch_add(1, Ordering::Relaxed);
        Some(i as u64)
    }

    /// Returns `None` if no such file
    async fn get_or_insert_and_ref(
        &self,
        repo: &ReadonlyRepo,
        parent_ino: Inode,
        name: &RepoPathComponent,
    ) -> FsResult<Inode> {
        // Fast path
        if let Some(i) = self.get_and_ref(parent_ino, name).await {
            return Ok(i);
        }

        let mut inodes = self.inodes.write().await;
        {
            let Some(children) = &inodes
                .get_mut(parent_ino as usize)
                .unwrap()
                .mutable_state
                .get_mut()
                .children
            else {
                return Err(ENOTDIR);
            };
            // Guard against races with another identical call to this method
            if let Some(i) = children.get(name).copied() {
                inodes
                    .get(i)
                    .unwrap()
                    .references
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(i as u64);
            }
        }

        // Create a child inode
        let parent = inodes.get_mut(parent_ino as usize).unwrap();
        let TreeValue::Tree(parent_tree_id) = parent.mutable_state.get_mut().value.clone() else {
            unreachable!()
        };

        let child_path = parent.path.join(name);
        let parent_tree = repo
            .store()
            .get_tree(parent.path.clone(), &parent_tree_id)
            .await
            .map_err(|e| {
                error!("accessing parent directory {:?}: {:#}", parent.path, e);
                EIO
            })?;
        let value = parent_tree.value(name).ok_or(ENOENT)?;
        let mut inode = InodeState::new(child_path, value.clone());
        inode.parent = Some(InodeParent {
            child_name: name.to_owned(),
            parent: parent_ino as usize,
        });
        let n = inodes.insert(inode);
        trace!("{:?} is inode {}", inodes.get(n).unwrap().path, n);
        inodes
            .get_mut(parent_ino as usize)
            .unwrap()
            .mutable_state
            .get_mut()
            .children
            .as_mut()
            .unwrap()
            .insert(name.to_owned(), n);
        Ok(n as u64)
    }

    async fn forget(&self, i: Inode, nlookup: u64) {
        let i = i as usize;
        let prev = self
            .inodes
            .read()
            .await
            .get(i)
            .unwrap()
            .references
            .fetch_sub(nlookup, Ordering::Relaxed);
        if prev > nlookup {
            return;
        }
        let mut inodes = self.inodes.write().await;
        let inode = inodes.remove(i);
        trace!("deallocated inode {}, formerly {:?}", i, inode.path);

        // Remove from parent's list of children
        if let Some(parent) = inode.parent {
            let parent_child = inodes
                .get_mut(parent.parent)
                .unwrap()
                .mutable_state
                .get_mut()
                .children
                .as_mut()
                .unwrap()
                .remove(&*parent.child_name)
                .unwrap();
            debug_assert_eq!(parent_child, i);
        }
    }

    async fn insert(
        &self,
        store: &Arc<Store>,
        parent: Inode,
        name: &RepoPathComponent,
        value: TreeValue,
    ) -> BackendResult<Inode> {
        let mut inodes = self.inodes.write().await;
        let full_path = inodes.get(parent as usize).unwrap().path.join(name);
        let mut state = InodeState::new(full_path, value.clone());
        state.parent = Some(InodeParent {
            child_name: name.to_owned(),
            parent: parent as usize,
        });
        let inode = inodes.insert(state);
        inodes
            .get_mut(parent as usize)
            .unwrap()
            .mutable_state
            .get_mut()
            .children
            .as_mut()
            .unwrap()
            .insert(name.to_owned(), inode);

        update_trees_locked(&inodes, store, inode).await?;

        Ok(inode as u64)
    }

    async fn update_trees(&self, store: &Arc<Store>, inode: Inode) -> BackendResult<()> {
        let inodes = self.inodes.read().await;
        update_trees_locked(&inodes, store, inode as usize).await
    }

    async fn trunc(&self, store: &Arc<Store>, inode: Inode, size: u64) -> BackendResult<()> {
        let inodes = self.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        let mut mutable = state.mutable_state.write().await;
        let TreeValue::File { id, .. } = &mut mutable.value else {
            unreachable!()
        };
        *id = match size {
            // Fast path
            0 => store.write_file(&state.path, &mut &[][..]).await?,
            _ => {
                let orig = store.read_file(&state.path, id).await?;
                let mut truncated = Truncate {
                    inner: orig,
                    remaining: size,
                };
                store.write_file(&state.path, &mut truncated).await?
            }
        };
        drop(mutable);
        update_trees_locked(&inodes, store, inode as usize).await?;
        Ok(())
    }
}

pin_project! {
    struct Truncate<T> {
        #[pin]
        inner: T,
        remaining: u64,
    }
}

impl<T: AsyncRead> AsyncRead for Truncate<T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        let max = Ord::max(
            buf.len(),
            usize::try_from(*this.remaining).unwrap_or(usize::MAX),
        );
        if max == 0 {
            return Poll::Ready(Ok(0));
        }
        let n = ready!(this.inner.poll_read(cx, &mut buf[..max]))?;
        if n > 0 {
            *this.remaining -= n as u64;
            return Poll::Ready(Ok(n));
        }
        // Zero-pad
        buf[..max].fill(0);
        *this.remaining -= max as u64;
        Poll::Ready(Ok(max))
    }
}

/// Propagate a change to `inode` up to the root tree
async fn update_trees_locked(
    inodes: &Slab<InodeState>,
    store: &Arc<Store>,
    inode: usize,
) -> BackendResult<()> {
    let state = inodes.get(inode as usize).unwrap();
    let Some(mut parent_info) = state.parent.clone() else {
        // `inode` is the root; there's nowhere to propagate
        return Ok(());
    };
    let mut value = state.mutable_state.read().await.value.clone();

    loop {
        let parent = inodes.get(parent_info.parent).unwrap();
        let mut parent_state = parent.mutable_state.write().await;
        let TreeValue::Tree(id) = &mut parent_state.value else {
            unreachable!()
        };
        let tree = store.get_tree(parent.path.clone(), id).await?;
        let mut entries = tree
            .entries_non_recursive()
            .map(|e| (e.name().to_owned(), e.value().clone()))
            .collect::<Vec<_>>();
        match entries.binary_search_by_key(&&*parent_info.child_name, |(n, _)| &*n) {
            Ok(i) => {
                // Replace existing entry
                entries[i].1 = value;
            }
            Err(i) => {
                // Insert new entry
                entries.insert(i, (parent_info.child_name, value));
            }
        };
        let new_tree = Tree::from_sorted_entries(entries);
        let new_tree_id = store.write_tree(&parent.path, new_tree).await?.id().clone();
        value = TreeValue::Tree(new_tree_id);
        parent_state.value = value.clone();

        if let Some(next) = &parent.parent {
            parent_info = next.clone();
        } else {
            return Ok(());
        }
    }
}

struct InodeState {
    path: RepoPathBuf,
    mutable_state: RwLock<InodeMutableState>,
    references: AtomicU64,
    parent: Option<InodeParent>,
}

impl InodeState {
    fn new(path: RepoPathBuf, value: TreeValue) -> Self {
        let children = match value {
            TreeValue::Tree(_) => Some(FxHashMap::default()),
            _ => None,
        };
        Self {
            path,
            mutable_state: RwLock::new(InodeMutableState { value, children }),
            references: AtomicU64::new(1),
            parent: None,
        }
    }
}

struct InodeMutableState {
    /// May diverge from what's recorded in the parent until flushed
    value: TreeValue,
    /// Populated iff value is Tree
    children: Option<FxHashMap<RepoPathComponentBuf, usize>>,
}

fn value_ty(value: &TreeValue) -> FileType {
    match value {
        TreeValue::File { .. } => FileType::RegularFile,
        TreeValue::Symlink(_) => FileType::Symlink,
        TreeValue::Tree(_) => FileType::Directory,
        TreeValue::GitSubmodule(_) => FileType::Directory,
    }
}

fn value_stat(value: &TreeValue) -> FileAttr {
    let ty = value_ty(value);
    let mut mode = 0o664;
    mode |= match value {
        TreeValue::File {
            executable: true, ..
        }
        | TreeValue::Tree(_) => 0o111,
        _ => 0,
    };
    FileAttr {
        ino: 0,
        size: 0,
        blocks: 0,
        atime: Timestamp::default(),
        mtime: Timestamp::default(),
        ctime: Timestamp::default(),
        mode: ty.to_mode() | mode,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 512,
    }
}

#[derive(Clone)]
struct InodeParent {
    child_name: RepoPathComponentBuf,
    parent: usize,
}

pin_project! {
    struct Overwrite<'a, T> {
        #[pin]
        inner: T,
        offset: u64,
        data: &'a [u8]
    }
}

impl<T: AsyncRead> AsyncRead for Overwrite<'_, T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();
        let n = ready!(this.inner.as_mut().poll_read(cx, buf))?;
        if let Some(overshoot) = (n as u64).checked_sub(*this.offset) {
            let overlap = Ord::min(overshoot, this.data.len() as u64);
            let overlap_end = *this.offset + overlap;
            let (copied, remaining) = this.data.split_at(overlap as usize);
            buf[*this.offset as usize..overlap_end as usize].copy_from_slice(copied);
            *this.offset = 0;
            *this.data = remaining;
        }
        if n > 0 {
            return Poll::Ready(Ok(n));
        }
        // Inner exhausted, finish up with the tail of data.
        let overflow = Ord::min(this.data.len(), buf.len());
        let (copied, remaining) = this.data.split_at(overflow);
        buf[..overflow].copy_from_slice(copied);
        *this.data = remaining;
        Poll::Ready(Ok(overflow))
    }
}

fn translate_name(name: &OsStr) -> FsResult<&RepoPathComponent> {
    Ok(RepoPathComponent::new(name.to_str().ok_or_else(|| {
        error!("non-Unicode name: {name:?}");
        EINVAL
    })?)
    .map_err(|e| {
        error!("illegal name: {name:?}: {e:#}");
        EINVAL
    })?)
}

const TTL: Duration = Duration::from_secs(60);
