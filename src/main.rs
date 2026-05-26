mod dir;
mod dir_fd;

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
    DirectoryEntry, DirectoryEntryPlus, EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, FileAttr,
    FileType, FsResult, Inode, MountOptions, ReplyAttr, ReplyCreate, ReplyEntry, ReplyOpen,
    ReplyReadlink, ReplyStatfs, Request,
};
use futures_util::AsyncRead;
use futures_util::io::AsyncReadExt;
use jj_lib::backend::{
    BackendResult, CommitId, CopyId, FileId, SymlinkId, Tree, TreeId, TreeValue,
};
use jj_lib::commit::Commit;
use jj_lib::merged_tree::MergedTree;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo, StoreFactories};
use jj_lib::repo_path::InvalidRepoPathComponentError;
use jj_lib::repo_path::RepoPath;
use jj_lib::repo_path::{RepoPathBuf, RepoPathComponent, RepoPathComponentBuf};
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use libc::{O_TRUNC, RENAME_EXCHANGE, RENAME_NOREPLACE};
use pin_project_lite::pin_project;
use rustc_hash::FxHashMap;
use slab::Slab;
use tokio::sync::RwLock;
use tracing::{error, trace};

use crate::dir::Dir;
use crate::dir_fd::DirFd;

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
        .block_on(Fs::new(&args.repository, args.mountpoint.clone()))
        .context("opening repository")?;
    let shared = fs.shared.clone();
    trace!("mounting");
    let session = fractal_fuse::Session::new(args.mountpoint, MountOptions::new().fs_name("jj"))
        .context("mounting")?
        .queue_depth(128);
    let notifier = session.notifier();
    ctrlc::set_handler(move || notifier.shutdown()).unwrap();
    trace!("running");
    let result = session.run(fs);
    trace!("shutting down");
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
    async fn new(path: &Path, underlying_dir: PathBuf) -> anyhow::Result<Self> {
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

        Ok(Self {
            shared: Arc::new(Shared::new(repo, commit, underlying_dir)?),
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
        let inode = self.shared.get_or_insert_and_ref(parent, name).await?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: self.shared.stat(inode).await?,
            generation: 0,
        })
    }

    fn forget(&self, _: Request, inode: Inode, nlookup: u64) {
        let shared = self.shared.clone();
        compio_runtime::spawn(async move {
            shared.forget(inode, nlookup).await;
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
            attr: self.shared.stat(inode).await?,
        })
    }

    async fn open(&self, _req: Request, inode: Inode, flags: u32) -> FsResult<ReplyOpen> {
        if flags & O_TRUNC as u32 != 0 {
            if let Err(e) = self.shared.trunc(inode, 0).await {
                let path = self.shared.get_path(inode).await;
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
        let path = self.shared.get_path(parent).await.join(name);
        let id = self
            .shared
            .store
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
        let attr = self.shared.insert(parent, name, value).await?;
        Ok(ReplyCreate {
            ttl: TTL,
            attr,
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
        self.shared.read(inode, offset, buf).await
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
        let inodes = self.shared.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        let mut mutable_state = state.mutable_state.write().await;
        let InodeData::File(FileInode {
            id,
            executable,
            copy_id,
        }) = &*mutable_state
        else {
            return Err(EISDIR);
        };
        let executable = *executable;
        let copy_id = copy_id.clone();
        let file = self
            .shared
            .store
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
            .store
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
        *mutable_state = InodeData::File(FileInode {
            id: updated_file,
            executable,
            copy_id,
        });
        drop(mutable_state);

        self.shared.update_trees(inode).await.map_err(|e| {
            error!(
                "updating directory metadata leading to written file {:?}: {e:#}",
                state.path
            );
            EIO
        })?;

        Ok(data.len())
    }

    async fn flush(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _lock_owner: u64,
    ) -> FsResult<()> {
        self.shared.write_commit().await.map_err(|e| {
            error!("committing for flush: {e:#}");
            EIO
        })?;
        Ok(())
    }

    async fn fsync(&self, _req: Request, _inode: Inode, _fh: u64, _datasync: bool) -> FsResult<()> {
        self.shared.write_commit().await.map_err(|e| {
            error!("committing for fsync: {e:#}");
            EIO
        })?;
        Ok(())
    }

    async fn fsyncdir(
        &self,
        _req: Request,
        _inode: Inode,
        _fh: u64,
        _datasync: bool,
    ) -> FsResult<()> {
        self.shared.write_commit().await.map_err(|e| {
            error!("committing for fsyncdir: {e:#}");
            EIO
        })?;
        Ok(())
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> FsResult<ReplyEntry> {
        let parent_path = self.shared.get_path(parent).await;
        let name = translate_name(name)?;
        let path = parent_path.join(name);
        let id = self
            .shared
            .store
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
        let attr = self.shared.insert(parent, name, value).await?;

        self.shared.write_commit().await.map_err(|e| {
            error!("committing new symlink: {e:#}");
            EIO
        })?;

        Ok(ReplyEntry {
            ttl: TTL,
            attr,
            generation: 0,
        })
    }

    async fn readlink(&self, _req: Request, inode: Inode) -> FsResult<ReplyReadlink> {
        let target = self.shared.readlink(inode).await?;
        Ok(ReplyReadlink { data: target })
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        start_offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        self.shared
            .readdir(inode, |kind, _, name, offset| {
                (offset > start_offset).then(|| DirectoryEntry {
                    ino: 0,
                    offset,
                    kind,
                    name: name.as_bytes().to_vec(),
                })
            })
            .await
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        start_offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        self.shared
            .readdir(inode, |kind, attr, name, offset| {
                if offset <= start_offset {
                    return None;
                }
                Some(DirectoryEntryPlus {
                    ino: 0,
                    offset,
                    kind,
                    name: name.as_bytes().to_vec(),
                    entry_ttl: TTL,
                    attr,
                    generation: 0,
                })
            })
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
        let value = TreeValue::Tree(self.shared.store.empty_tree_id().clone());
        let attr = self.shared.insert(parent, name, value).await?;

        self.shared.write_commit().await.map_err(|e| {
            error!("committing new directory: {e:#}");
            EIO
        })?;

        Ok(ReplyEntry {
            ttl: TTL,
            attr,
            generation: 0,
        })
    }

    async fn unlink(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<()> {
        let name = translate_name(name)?;
        if let Err(e) = self.shared.remove(parent, name).await {
            let path = self.shared.get_path(parent).await.join(name);
            error!("removing {path:?}: {e:#}");
            return Err(EIO);
        }

        if let Err(e) = self.shared.write_commit().await {
            let path = self.shared.get_path(parent).await.join(name);
            error!("committing removal of {path:?}: {e:#}");
            return Err(EIO);
        }

        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<()> {
        let name = translate_name(name)?;
        if let Err(e) = self.shared.remove(parent, name).await {
            let path = self.shared.get_path(parent).await.join(name);
            error!("removing {path:?}: {e:#}");
            return Err(EIO);
        }

        if let Err(e) = self.shared.write_commit().await {
            let path = self.shared.get_path(parent).await.join(name);
            error!("committing removal of {path:?}: {e:#}");
            return Err(EIO);
        }

        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: u32,
    ) -> FsResult<()> {
        let name = translate_name(name)?;
        let new_name = translate_name(new_name)?;
        let mut inodes = self.shared.inodes.write().await; // Write lock for atomicity

        // Read target side
        let orig_dst_tree;
        let prev_value;
        {
            let new_parent_state = inodes.get_mut(new_parent as usize).unwrap();
            let new_mutable = new_parent_state.mutable_state.get_mut();
            let Some(TreeInode { id, .. }) = new_mutable.as_tree() else {
                unreachable!()
            };

            orig_dst_tree = self
                .shared
                .store
                .get_tree(new_parent_state.path.clone(), id)
                .await
                .map_err(|e| {
                    error!(
                        "accessing tree at {:?} for rename: {e:#}",
                        new_parent_state.path
                    );
                    EIO
                })?;
            prev_value = orig_dst_tree.value(new_name);

            if flags & RENAME_NOREPLACE != 0 && prev_value.is_some() {
                return Err(EEXIST);
            }
        }

        // Read source side
        let mut orig_src_tree;
        let inode = {
            let parent_state = inodes.get_mut(parent as usize).unwrap();
            let mutable = parent_state.mutable_state.get_mut();
            let Some(TreeInode { id, children }) = mutable.as_tree_mut() else {
                unreachable!()
            };
            orig_src_tree = self
                .shared
                .store
                .get_tree(parent_state.path.clone(), id)
                .await
                .map_err(|e| {
                    error!(
                        "accessing tree at {:?} for rename: {e:#}",
                        parent_state.path
                    );
                    EIO
                })?;

            children.remove(name).unwrap()
        };

        // Write target side
        let prev_inode;
        {
            let new_parent_state = inodes.get_mut(new_parent as usize).unwrap();
            let new_mutable = new_parent_state
                .mutable_state
                .get_mut()
                .as_tree_mut()
                .unwrap();

            // Write inode
            prev_inode = new_mutable.children.insert(new_name.to_owned(), inode);

            // Write tree entry
            let updated_dst_tree = tree_insert(
                &orig_dst_tree,
                new_name,
                orig_src_tree.value(name).unwrap().clone(),
            );
            let updated_dst_tree = self
                .shared
                .store
                .write_tree(&new_parent_state.path, updated_dst_tree)
                .await
                .map_err(|e| {
                    error!("updating tree {:?} for rename: {e:#}", orig_dst_tree.dir());
                    EIO
                })?;
            new_mutable.id = updated_dst_tree.id().clone();
            // Don't clobber the new file if this is a rename within the same dir
            if parent == new_parent {
                orig_src_tree = updated_dst_tree;
            }
        };

        // Write source side
        {
            let parent_state = inodes.get_mut(parent as usize).unwrap();
            let mutable = parent_state.mutable_state.get_mut().as_tree_mut().unwrap();
            if flags & RENAME_EXCHANGE != 0
                && let Some(prev_inode) = prev_inode
            {
                mutable.children.insert(name.to_owned(), prev_inode);
            }
            let updated_src_tree = match (flags & RENAME_EXCHANGE != 0, prev_value) {
                (true, Some(prev_value)) => tree_insert(&orig_src_tree, name, prev_value.clone()),
                _ => tree_remove(&orig_src_tree, name),
            };
            let updated_src_tree = self
                .shared
                .store
                .write_tree(&parent_state.path, updated_src_tree)
                .await
                .map_err(|e| {
                    error!("updating tree {:?} for rename: {e:#}", orig_src_tree.dir());
                    EIO
                })?;
            mutable.id = updated_src_tree.id().clone();
        }

        update_trees_locked(&inodes, &self.shared.store, parent as usize)
            .await
            .map_err(|e| {
                error!(
                    "updating directory metadata leading to rename from {:?}: {e:#}",
                    orig_src_tree.dir().join(name)
                );
                EIO
            })?;

        if parent != new_parent {
            update_trees_locked(&inodes, &self.shared.store, new_parent as usize)
                .await
                .map_err(|e| {
                    error!(
                        "updating directory metadata leading to rename to {:?}: {e:#}",
                        orig_dst_tree.dir().join(new_name)
                    );
                    EIO
                })?;
        }

        drop(inodes);
        self.shared.write_commit().await.map_err(|e| {
            error!(
                "committing rename from {:?} to {:?}: {e:#}",
                orig_src_tree.dir().join(name),
                orig_dst_tree.dir().join(new_name)
            );
            EIO
        })?;

        Ok(())
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
    store: Arc<Store>,
    repo_state: RwLock<RepoState>,
    inodes: RwLock<Slab<InodeState>>,
    underlying_dir: DirFd,
}

impl Shared {
    fn new(
        repo: Arc<ReadonlyRepo>,
        commit: Commit,
        underlying_dir_path: PathBuf,
    ) -> anyhow::Result<Self> {
        let Some(root_id) = commit.tree_ids().as_resolved().cloned() else {
            bail!("conflicted trees are not implemented");
        };
        let root_inode =
            InodeState::from_tree_value(RepoPathBuf::root(), TreeValue::Tree(root_id.clone()));
        let underlying_dir =
            DirFd::open(&underlying_dir_path).context("opening underlying directory")?;
        Ok(Self {
            store: repo.store().clone(),
            repo_state: RwLock::new(RepoState { repo, commit }),
            inodes: RwLock::new(Slab::from_iter([
                // Dummy inode to reserve slot 0
                (
                    0,
                    InodeState::from_tree_value(RepoPathBuf::root(), TreeValue::Tree(root_id)),
                ),
                (FUSE_ROOT_ID as usize, root_inode),
            ])),
            underlying_dir,
        })
    }

    async fn write_commit(&self) -> anyhow::Result<()> {
        let Some(TreeValue::Tree(id)) = self.get(FUSE_ROOT_ID).await else {
            return Ok(());
        };
        // Rewrite the commit with the current tree
        let mut state = self.repo_state.write().await;
        let mut tx = state.repo.start_transaction();
        let new_commit = tx
            .repo_mut()
            .rewrite_commit(&state.commit)
            .set_tree(MergedTree::resolved(self.store.clone(), id))
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

    async fn get(&self, inode: Inode) -> Option<TreeValue> {
        self.inodes
            .read()
            .await
            .get(inode as usize)
            .unwrap()
            .mutable_state
            .read()
            .await
            .to_tree_value()
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

    async fn get_path_value(&self, inode: Inode) -> (RepoPathBuf, Option<TreeValue>) {
        let inodes = self.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        (
            state.path.clone(),
            state.mutable_state.read().await.to_tree_value(),
        )
    }

    async fn readlink(&self, inode: Inode) -> FsResult<Vec<u8>> {
        let inodes = self.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        Ok(match &*state.mutable_state.read().await {
            InodeData::Symlink(id) => {
                let target = self
                    .store
                    .read_symlink(&state.path, id)
                    .await
                    .map_err(|e| {
                        error!("reading symlink {:?}: {e:#}", state.path);
                        EIO
                    })?;
                target.into_bytes()
            }
            InodeData::IgnoredFile => {
                let path = to_relative_fs_path(&state.path).map_err(|e| {
                    error!("illegal path {:?}: {e:#}", state.path);
                    EIO
                })?;
                let target = self.underlying_dir.readlink_child(&path).map_err(|e| {
                    error!("reading symlink {:?}: {e}", state.path);
                    EIO
                })?;
                target
            }
            _ => return Err(EINVAL),
        })
    }

    async fn read(&self, inode: Inode, mut offset: u64, buf: &mut [u8]) -> FsResult<usize> {
        let (path, Some(TreeValue::File { id, .. })) = self.get_path_value(inode).await else {
            return Err(EISDIR);
        };
        let mut file = self.store.read_file(&path, &id).await.map_err(|e| {
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

    async fn stat(&self, inode: Inode) -> FsResult<FileAttr> {
        Ok(FileAttr {
            ino: inode,
            ..self
                .inodes
                .read()
                .await
                .get(inode as usize)
                .unwrap()
                .stat(&self.store, &self.underlying_dir)
                .await?
        })
    }

    async fn readdir<T, F>(&self, inode: Inode, mut f: F) -> FsResult<Vec<T>>
    where
        F: FnMut(FileType, FileAttr, &str, u64) -> Option<T>,
    {
        // Future work: We could skip stat to speed up regular `readdir`, maybe by replacing `F`
        // with a custom trait. We could also skip visiting elements outside the readdir offset/size
        // range, but it's unclear if this is worth the trouble.
        let mut entries = Vec::new();
        let dir_path;
        let tree_id;
        {
            let inodes = self.inodes.read().await;
            let state = inodes.get(inode as usize).unwrap();
            dir_path = state.path.clone();
            let attr = FileAttr {
                ino: inode,
                ..state
                    .stat(&self.store, &self.underlying_dir)
                    .await
                    .map_err(|e| {
                        error!("looking up metadata for {dir_path:?}: {e:#}");
                        EIO
                    })?
            };
            let value = &*state.mutable_state.read().await;
            tree_id = match value {
                InodeData::Tree(tree_inode) => Some(tree_inode.id.clone()),
                InodeData::IgnoredTree(_) => None,
                _ => return Err(ENOTDIR),
            };
            entries.extend(f(FileType::Directory, attr, ".", 1));
            if let Some(p) = &state.parent {
                let parent_attr = FileAttr {
                    ino: p.parent as u64,
                    ..inodes
                        .get(p.parent)
                        .unwrap()
                        .stat(&self.store, &self.underlying_dir)
                        .await
                        .map_err(|e| {
                            error!("looking up metadata for {dir_path:?}: {e:#}");
                            EIO
                        })?
                };
                entries.extend(f(FileType::Directory, parent_attr, "..", 2));
            }
        }

        // Add entries from repo
        if let Some(tree_id) = tree_id {
            let tree = self
                .store
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
            for entry in iter {
                let name = match entry.name().to_fs_name() {
                    Ok(name) => name,
                    Err(e) => {
                        error!("hiding unrepresentable name {:?}: {:#}", entry.name(), e);
                        continue;
                    }
                };
                let n = entries.len();
                let entry_path = dir_path.join(entry.name());
                let attr = value_stat(&self.store, &entry_path, entry.value())
                    .await
                    .map_err(|e| {
                        error!("looking up metadata for directory entry {entry_path:?}: {e:#}");
                        EIO
                    })?;
                entries.extend(f(value_ty(entry.value()), attr, name, n as u64 + 1));
            }
        }

        // Add untracked entries
        let dir_path = to_relative_fs_path(&dir_path).map_err(|e| {
            error!("illegal path {dir_path:?}: {e:#}");
            EIO
        })?;
        match self.underlying_dir.open_child(&dir_path) {
            Ok(fd) => {
                let iter = Dir::new(fd).map_err(|e| {
                    error!("accessing underlying directory for {dir_path:?}: {e}");
                    EIO
                })?;
                for entry in iter {
                    let entry = match entry {
                        Ok(x) => x,
                        Err(e) => {
                            error!("reading underlying directory for {dir_path:?}: {e}");
                            break;
                        }
                    };
                    let name = match str::from_utf8(&entry.name) {
                        Ok(x) => x,
                        Err(e) => {
                            error!(
                                "hiding unrepresentable name {:?}: {:#}",
                                entry.name.escape_ascii(),
                                e
                            );
                            continue;
                        }
                    };
                    let n = entries.len();
                    let path = dir_path.join(name);
                    let attr = self.underlying_dir.stat_child(&path).map_err(|e| {
                        error!("statting {:?}: {e}", path);
                        EIO
                    })?;
                    entries.extend(f(entry.ty, attr, name, n as u64 + 1));
                }
            }
            // No underlying files
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                error!("accessing underlying directory for {dir_path:?}: {e}");
            }
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
            .as_tree()?
            .children
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
        parent_ino: Inode,
        name: &RepoPathComponent,
    ) -> FsResult<Inode> {
        // Fast path
        if let Some(i) = self.get_and_ref(parent_ino, name).await {
            return Ok(i);
        }

        let mut inodes = self.inodes.write().await;
        {
            let Some(tree) = &inodes
                .get_mut(parent_ino as usize)
                .unwrap()
                .mutable_state
                .get_mut()
                .as_tree()
            else {
                return Err(ENOTDIR);
            };
            // Guard against races with another identical call to this method
            if let Some(i) = tree.children.get(name).copied() {
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
        let Some(parent_tree_id) = parent
            .mutable_state
            .get_mut()
            .as_tree()
            .map(|x| x.id.clone())
        else {
            // TODO: Handle untracked dirs
            unreachable!()
        };

        let child_path = parent.path.join(name);
        let parent_tree = self
            .store
            .get_tree(parent.path.clone(), &parent_tree_id)
            .await
            .map_err(|e| {
                error!("accessing parent directory {:?}: {:#}", parent.path, e);
                EIO
            })?;
        let inode = match parent_tree.value(name) {
            Some(value) => {
                let mut inode = InodeState::from_tree_value(child_path, value.clone());
                inode.parent = Some(InodeParent {
                    child_name: name.to_owned(),
                    parent: parent_ino as usize,
                });
                inode
            }
            None => {
                let child_fs_path = to_relative_fs_path(&child_path).map_err(|e| {
                    error!("unrepresentable path {child_path:?}: {e}");
                    EINVAL
                })?;
                let attr = self
                    .underlying_dir
                    .stat_child(&child_fs_path)
                    .map_err(|e| {
                        error!("stat {}: {e}", child_fs_path.display());
                        e.raw_os_error().unwrap_or(EIO)
                    })?;
                let data = match attr.mode & FileType::Directory.to_mode() != 0 {
                    true => InodeData::IgnoredTree(FxHashMap::default()),
                    false => InodeData::IgnoredFile,
                };
                InodeState::new(child_path, data)
            }
        };
        let n = inodes.insert(inode);
        trace!("{:?} is inode {}", inodes.get(n).unwrap().path, n);
        inodes
            .get_mut(parent_ino as usize)
            .unwrap()
            .mutable_state
            .get_mut()
            .as_tree_mut()
            .unwrap()
            .children
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
                .as_tree_mut()
                .unwrap()
                .children
                .remove(&*parent.child_name)
                .unwrap();
            debug_assert_eq!(parent_child, i);
        }
    }

    async fn insert(
        &self,
        parent: Inode,
        name: &RepoPathComponent,
        value: TreeValue,
    ) -> FsResult<FileAttr> {
        let mut inodes = self.inodes.write().await;
        let full_path = inodes.get(parent as usize).unwrap().path.join(name);
        let mut state = InodeState::from_tree_value(full_path, value.clone());
        state.parent = Some(InodeParent {
            child_name: name.to_owned(),
            parent: parent as usize,
        });
        let inode = inodes.insert(state);
        let inode_ref = inodes.get_mut(parent as usize).unwrap();
        inode_ref
            .mutable_state
            .get_mut()
            .as_tree_mut()
            .unwrap()
            .children
            .insert(name.to_owned(), inode);
        let attr = FileAttr {
            ino: inode as u64,
            ..inode_ref.stat(&self.store, &self.underlying_dir).await?
        };

        update_trees_locked(&inodes, &self.store, inode)
            .await
            .map_err(|e| {
                error!("updating trees for new inode: {e:#}");
                EIO
            })?;

        Ok(attr)
    }

    async fn remove(&self, parent: Inode, name: &RepoPathComponent) -> BackendResult<()> {
        let inodes = self.inodes.read().await;
        let state = inodes.get(parent as usize).unwrap();
        // Hold a write lock for the duration so racing metadata ops don't lose data
        let mut mutable = state.mutable_state.write().await;
        let Some(TreeInode { id, .. }) = mutable.as_tree_mut() else {
            unreachable!()
        };
        let tree = self.store.get_tree(state.path.clone(), id).await?;
        let new_tree = tree_remove(&tree, name);
        *id = self
            .store
            .write_tree(&state.path, new_tree)
            .await?
            .id()
            .clone();

        update_trees_locked(&inodes, &self.store, parent as usize).await?;

        Ok(())
    }

    async fn update_trees(&self, inode: Inode) -> BackendResult<()> {
        let inodes = self.inodes.read().await;
        update_trees_locked(&inodes, &self.store, inode as usize).await
    }

    async fn trunc(&self, inode: Inode, size: u64) -> BackendResult<()> {
        let inodes = self.inodes.read().await;
        let state = inodes.get(inode as usize).unwrap();
        let mut mutable = state.mutable_state.write().await;
        let Some(FileInode { id, .. }) = mutable.as_file_mut() else {
            unreachable!()
        };
        *id = match size {
            // Fast path
            0 => self.store.write_file(&state.path, &mut &[][..]).await?,
            _ => {
                let orig = self.store.read_file(&state.path, id).await?;
                let mut truncated = Truncate {
                    inner: orig,
                    remaining: size,
                };
                self.store.write_file(&state.path, &mut truncated).await?
            }
        };
        drop(mutable);
        update_trees_locked(&inodes, &self.store, inode as usize).await?;
        Ok(())
    }
}

/// Identifies the committed state a VFS view is based on
struct RepoState {
    repo: Arc<ReadonlyRepo>,
    commit: Commit,
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
    let Some(mut value) = state.mutable_state.read().await.to_tree_value() else {
        // Untracked file; short-circuit
        return Ok(());
    };

    loop {
        let parent = inodes.get(parent_info.parent).unwrap();
        let mut parent_state = parent.mutable_state.write().await;
        let parent_state = parent_state.as_tree_mut().unwrap();
        let tree = store
            .get_tree(parent.path.clone(), &parent_state.id)
            .await?;
        let new_tree = tree_insert(&tree, &parent_info.child_name, value);
        let new_tree_id = store.write_tree(&parent.path, new_tree).await?.id().clone();
        value = TreeValue::Tree(new_tree_id.clone());
        parent_state.id = new_tree_id;

        if let Some(next) = &parent.parent {
            parent_info = next.clone();
        } else {
            return Ok(());
        }
    }
}

fn tree_insert(tree: &jj_lib::tree::Tree, name: &RepoPathComponent, value: TreeValue) -> Tree {
    let mut entries = tree
        .entries_non_recursive()
        .map(|e| (e.name().to_owned(), e.value().clone()))
        .collect::<Vec<_>>();
    match entries.binary_search_by_key(&name, |(n, _)| &*n) {
        Ok(i) => {
            // Replace existing entry
            entries[i].1 = value;
        }
        Err(i) => {
            // Insert new entry
            entries.insert(i, (name.to_owned(), value));
        }
    };
    Tree::from_sorted_entries(entries)
}

fn tree_remove(tree: &jj_lib::tree::Tree, name: &RepoPathComponent) -> Tree {
    let entries = tree
        .entries_non_recursive()
        .filter(|e| e.name() != name)
        .map(|e| (e.name().to_owned(), e.value().clone()))
        .collect();
    Tree::from_sorted_entries(entries)
}

struct InodeState {
    path: RepoPathBuf,
    mutable_state: RwLock<InodeData>,
    references: AtomicU64,
    parent: Option<InodeParent>,
}

impl InodeState {
    fn new(path: RepoPathBuf, state: InodeData) -> Self {
        Self {
            path,
            mutable_state: RwLock::new(state),
            references: AtomicU64::new(1),
            parent: None,
        }
    }

    fn from_tree_value(path: RepoPathBuf, value: TreeValue) -> Self {
        Self::new(path, InodeData::from(value))
    }

    async fn stat(&self, store: &Arc<Store>, underlying_dir: &DirFd) -> FsResult<FileAttr> {
        const EXECUTABLE_MODE: u32 = 0o111;
        let mutable = self.mutable_state.read().await;
        let default_attr = FileAttr {
            mode: 0o664 | mutable.ty().map_or(0, FileType::to_mode),
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            ..Default::default()
        };
        Ok(match &*mutable {
            InodeData::File(file) => {
                let meta = store
                    .get_file_metadata(&self.path, &file.id)
                    .await
                    .map_err(|e| {
                        error!("fetching metadata for {:?}: {e:#}", self.path);
                        EIO
                    })?;
                let mut mode = 0;
                if file.executable {
                    mode |= EXECUTABLE_MODE;
                }
                FileAttr {
                    mode: mode | default_attr.mode,
                    size: meta.size,
                    ..default_attr
                }
            }
            InodeData::Tree(_) => FileAttr {
                mode: EXECUTABLE_MODE | default_attr.mode,
                ..default_attr
            },
            InodeData::IgnoredFile | InodeData::IgnoredTree(_) => {
                let path = to_relative_fs_path(&self.path).map_err(|e| {
                    error!("illegal path {:?}: {e:#}", self.path);
                    EIO
                })?;
                underlying_dir.stat_child(&path).map_err(|e| {
                    error!("statting {:?}: {e}", self.path);
                    EIO
                })?
            }
            _ => default_attr,
        })
    }
}

/// Data that an inode may represent
///
/// Corresponds to jj's [`TreeValue`], but with some extra state
#[derive(Debug)]
enum InodeData {
    Tree(TreeInode),
    File(FileInode),
    Symlink(SymlinkId),
    GitSubmodule(CommitId),
    IgnoredTree(FxHashMap<RepoPathComponentBuf, usize>),
    IgnoredFile,
}

impl InodeData {
    fn as_tree(&self) -> Option<&TreeInode> {
        match self {
            Self::Tree(x) => Some(x),
            _ => None,
        }
    }

    fn as_tree_mut(&mut self) -> Option<&mut TreeInode> {
        match self {
            Self::Tree(x) => Some(x),
            _ => None,
        }
    }

    #[expect(dead_code)]
    fn as_file(&self) -> Option<&FileInode> {
        match self {
            Self::File(x) => Some(x),
            _ => None,
        }
    }

    fn as_file_mut(&mut self) -> Option<&mut FileInode> {
        match self {
            Self::File(x) => Some(x),
            _ => None,
        }
    }

    fn ty(&self) -> Option<FileType> {
        Some(match self {
            InodeData::Tree(_) | InodeData::IgnoredTree(_) => FileType::Directory,
            InodeData::File(_) => FileType::RegularFile,
            InodeData::Symlink(_) => FileType::Symlink,
            InodeData::GitSubmodule(_) => FileType::Directory,
            InodeData::IgnoredFile => return None,
        })
    }

    fn to_tree_value(&self) -> Option<TreeValue> {
        Some(match self {
            InodeData::Tree(x) => TreeValue::Tree(x.id.clone()),
            InodeData::File(x) => TreeValue::File {
                id: x.id.clone(),
                executable: x.executable,
                copy_id: x.copy_id.clone(),
            },
            InodeData::Symlink(x) => TreeValue::Symlink(x.clone()),
            InodeData::GitSubmodule(x) => TreeValue::GitSubmodule(x.clone()),
            InodeData::IgnoredTree(_) | InodeData::IgnoredFile => return None,
        })
    }
}

impl From<TreeValue> for InodeData {
    fn from(value: TreeValue) -> Self {
        match value {
            TreeValue::File {
                id,
                executable,
                copy_id,
            } => InodeData::File(FileInode {
                id,
                executable,
                copy_id,
            }),
            TreeValue::Symlink(symlink_id) => InodeData::Symlink(symlink_id),
            TreeValue::Tree(tree_id) => InodeData::Tree(TreeInode {
                id: tree_id,
                children: FxHashMap::default(),
            }),
            TreeValue::GitSubmodule(x) => InodeData::GitSubmodule(x),
        }
    }
}

#[derive(Debug, Clone)]
struct TreeInode {
    id: TreeId,
    children: FxHashMap<RepoPathComponentBuf, usize>,
}

#[derive(Debug, Clone)]
struct FileInode {
    id: FileId,
    executable: bool,
    copy_id: CopyId,
}

fn value_ty(value: &TreeValue) -> FileType {
    match value {
        TreeValue::File { .. } => FileType::RegularFile,
        TreeValue::Symlink(_) => FileType::Symlink,
        TreeValue::Tree(_) => FileType::Directory,
        TreeValue::GitSubmodule(_) => FileType::Directory,
    }
}

async fn value_stat(
    store: &Arc<Store>,
    path: &RepoPath,
    value: &TreeValue,
) -> BackendResult<FileAttr> {
    let ty = value_ty(value);
    let mut mode = 0o664;
    mode |= match value {
        TreeValue::File {
            executable: true, ..
        }
        | TreeValue::Tree(_) => 0o111,
        _ => 0,
    };
    let mut size = 0;
    if let TreeValue::File { id, .. } = value {
        let meta = store.get_file_metadata(path, id).await?;
        size = meta.size;
    }
    Ok(FileAttr {
        mode: ty.to_mode() | mode,
        size,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        blksize: 512,
        ..Default::default()
    })
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

fn to_relative_fs_path(path: &RepoPath) -> Result<PathBuf, InvalidRepoPathComponentError> {
    let mut buf = PathBuf::with_capacity(path.as_internal_file_string().len());
    for component in path.components() {
        buf.push(component.to_fs_name()?);
    }
    if buf.as_os_str().is_empty() {
        buf.push(".");
    }
    Ok(buf)
}

const TTL: Duration = Duration::from_secs(60);
