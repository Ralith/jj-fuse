use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Poll, ready};
use std::time::Duration;
use std::{path::PathBuf, process::ExitCode};

use anyhow::{Context, anyhow, bail};
use clap::Parser;
use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, FileAttr, FileType,
    FsResult, Inode, MountOptions, ReplyAttr, ReplyEntry, ReplyOpen, ReplyReadlink, ReplyStatfs,
    Request, Timestamp,
};
use futures_util::AsyncRead;
use futures_util::io::AsyncReadExt;
use jj_lib::backend::{BackendResult, Tree, TreeId, TreeValue};
use jj_lib::commit::Commit;
use jj_lib::merged_tree::MergedTree;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo, StoreFactories};
use jj_lib::repo_path::{RepoPathBuf, RepoPathComponent, RepoPathComponentBuf};
use jj_lib::settings::UserSettings;
use pin_project_lite::pin_project;
use rustc_hash::FxHashMap;
use slab::Slab;
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
        eprintln!("{:#}", e);
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
    fractal_fuse::Session::new(args.mountpoint, MountOptions::new().fs_name("jj"))?
        .queue_depth(128)
        .run(fs)?;
    rt.block_on(shared.write_commit())
        .context("committing final state")?;
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
        let repo = self.shared.repo();
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
        self.shared.inodes.forget(inode, nlookup);
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
            attr: self.shared.inodes.stat(&self.shared.repo(), inode).await?,
        })
    }

    async fn open(&self, _req: Request, _inode: Inode, _flags: u32) -> FsResult<ReplyOpen> {
        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
            backing_id: 0,
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
            .read(&self.shared.repo(), inode, offset, buf)
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
        let inodes = self.shared.inodes.inodes.read().unwrap();
        let state = inodes.get(inode as usize).unwrap();
        let value = &mut state.mutable_state.write().unwrap().value;
        let TreeValue::File {
            id,
            executable,
            copy_id,
        } = &*value
        else {
            return Err(EISDIR);
        };
        let executable = *executable;
        let copy_id = copy_id.clone();
        let file = self
            .shared
            .repo()
            .store()
            .read_file(&state.path, id)
            .await
            .map_err(|e| {
                error!("opening {:?}: {:#}", state.path, e);
                EIO
            })?;
        trace!("opened old file {id}");
        // Create a new file with the specified data overwritten
        let updated_file = self
            .shared
            .repo()
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
        *value = TreeValue::File {
            id: updated_file,
            executable,
            copy_id,
        };

        Ok(data.len())
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> FsResult<ReplyEntry> {
        let parent_path = self.shared.inodes.get_path(parent);
        let name = RepoPathComponent::new(name.to_str().ok_or_else(|| {
            error!("non-Unicode symlink name: {name:?}");
            EINVAL
        })?)
        .map_err(|e| {
            error!("illegal symlink name: {name:?}: {e:#}");
            EINVAL
        })?;
        let path = parent_path.join(name);
        let id = self
            .shared
            .repo()
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
        let ino = self.shared.inodes.insert(parent, name, value);

        Ok(ReplyEntry {
            ttl: TTL,
            attr: FileAttr { ino, ..attr },
            generation: 0,
        })
    }

    async fn readlink(&self, _req: Request, inode: Inode) -> FsResult<ReplyReadlink> {
        let (path, TreeValue::Symlink(id)) = self.shared.inodes.get_path_value(inode) else {
            return Err(EINVAL);
        };
        let target = self
            .shared
            .repo()
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
                &self.shared.repo(),
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
                &self.shared.repo(),
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
    fn repo(&self) -> Arc<ReadonlyRepo> {
        self.repo_state.read().unwrap().repo.clone()
    }

    async fn write_commit(&self) -> anyhow::Result<()> {
        // Construct a tree with a recent state for every open inode
        let TreeValue::Tree(id) = self
            .inodes
            .flatten_value(&self.repo_state.read().unwrap().repo, FUSE_ROOT_ID)
            .await?
        else {
            // Root inode is always a tree
            unreachable!()
        };

        // Rewrite the commit with that tree
        let mut state = self.repo_state.write().unwrap();
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

    fn get_path(&self, inode: Inode) -> RepoPathBuf {
        self.inodes
            .read()
            .unwrap()
            .get(inode as usize)
            .unwrap()
            .path
            .clone()
    }

    fn get_path_value(&self, inode: Inode) -> (RepoPathBuf, TreeValue) {
        let inodes = self.inodes.read().unwrap();
        let state = inodes.get(inode as usize).unwrap();
        (
            state.path.clone(),
            state.mutable_state.read().unwrap().value.clone(),
        )
    }

    async fn read(
        &self,
        repo: &ReadonlyRepo,
        inode: Inode,
        mut offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        let (path, TreeValue::File { id, .. }) = self.get_path_value(inode) else {
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
        let (path, value) = self.get_path_value(inode);
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
            let inodes = self.inodes.read().unwrap();
            let state = inodes.get(inode as usize).unwrap();
            let value = state.mutable_state.read().unwrap().value.clone();
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
                        .unwrap()
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
    fn get_and_ref(&self, parent: Inode, name: &RepoPathComponent) -> Option<Inode> {
        let inodes = self.inodes.read().unwrap();
        let i = *inodes
            .get(parent as usize)?
            .mutable_state
            .read()
            .unwrap()
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
        if let Some(i) = self.get_and_ref(parent_ino, name) {
            return Ok(i);
        }

        let mut inodes = self.inodes.write().unwrap();
        {
            let Some(children) = &inodes
                .get_mut(parent_ino as usize)
                .unwrap()
                .mutable_state
                .get_mut()
                .unwrap()
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
        let TreeValue::Tree(parent_tree_id) = parent.mutable_state.get_mut().unwrap().value.clone()
        else {
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
            .unwrap()
            .children
            .as_mut()
            .unwrap()
            .insert(name.to_owned(), n);
        Ok(n as u64)
    }

    fn forget(&self, i: Inode, nlookup: u64) {
        let i = i as usize;
        let prev = self
            .inodes
            .read()
            .unwrap()
            .get(i)
            .unwrap()
            .references
            .fetch_sub(nlookup, Ordering::Relaxed);
        if prev > nlookup {
            return;
        }
        let mut inodes = self.inodes.write().unwrap();
        let inode = inodes.remove(i);
        trace!("deallocated inode {}, formerly {:?}", i, inode.path);

        // Remove from parent's list of children
        if let Some(parent) = inode.parent {
            let parent_child = inodes
                .get_mut(parent.parent)
                .unwrap()
                .mutable_state
                .get_mut()
                .unwrap()
                .children
                .as_mut()
                .unwrap()
                .remove(&*parent.child_name)
                .unwrap();
            debug_assert_eq!(parent_child, i);
        }
    }

    /// Obtain a `TreeValue` that independently represents a recent state of `inode`
    async fn flatten_value(&self, repo: &ReadonlyRepo, inode: Inode) -> BackendResult<TreeValue> {
        let (path, base, current_children) = {
            let inodes = self.inodes.read().unwrap();
            let state = inodes.get(inode as usize).unwrap();
            let mutable = state.mutable_state.read().unwrap();
            (
                state.path.clone(),
                mutable.value.clone(),
                mutable.children.clone(),
            )
        };
        // Non-tree inodes are self-contained (for now)
        let tree_id = match base {
            TreeValue::Tree(tree_id) => tree_id,
            _ => return Ok(base),
        };
        let tree = repo.store().get_tree(path.clone(), &tree_id).await?;
        let mut fresh_children = Vec::new();
        let mut current_children = current_children.unwrap();
        for entry in tree.entries_non_recursive() {
            if let Some((name, inode)) = current_children.remove_entry(entry.name()) {
                // Potentially edited child
                let flattened = Box::pin(self.flatten_value(repo, inode as Inode)).await?;
                fresh_children.push((name, flattened));
            } else {
                // Untouched child
                fresh_children.push((entry.name().to_owned(), entry.value().clone()));
            }
        }
        // New children
        for (name, inode) in current_children {
            let flattened = Box::pin(self.flatten_value(repo, inode as Inode)).await?;
            fresh_children.push((name, flattened));
        }
        fresh_children.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let fresh_tree = repo
            .store()
            .write_tree(&path, Tree::from_sorted_entries(fresh_children))
            .await?;
        Ok(TreeValue::Tree(fresh_tree.id().clone()))
    }

    fn insert(&self, parent: Inode, name: &RepoPathComponent, value: TreeValue) -> Inode {
        let mut inodes = self.inodes.write().unwrap();
        let full_path = inodes.get(parent as usize).unwrap().path.join(name);
        let mut state = InodeState::new(full_path, value);
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
            .unwrap()
            .children
            .as_mut()
            .unwrap()
            .insert(name.to_owned(), inode);
        inode as u64
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

const TTL: Duration = Duration::from_secs(60);
