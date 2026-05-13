use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Poll, ready};
use std::time::Duration;
use std::{path::PathBuf, process::ExitCode};

use anyhow::{Context, anyhow, bail};
use clap::Parser;
use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, EIO, EISDIR, ENOENT, ENOTDIR, FileAttr, FileType, FsResult,
    Inode, MountOptions, ReplyAttr, ReplyEntry, ReplyOpen, ReplyStatfs, Request, Timestamp,
};
use futures_util::AsyncRead;
use futures_util::io::AsyncReadExt;
use jj_lib::backend::{BackendResult, CopyId, FileId, TreeId, TreeValue};
use jj_lib::commit::Commit;
use jj_lib::merged_tree::MergedTree;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo, StoreFactories};
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponent, RepoPathComponentBuf};
use jj_lib::settings::UserSettings;
use jj_lib::tree::Tree;
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
    trace!("mounting");
    fractal_fuse::Session::new(MountOptions::new().fs_name("jj"))
        .queue_depth(128)
        .run(fs, &args.mountpoint)?;
    Ok(())
}

struct Fs {
    commit: Mutex<Commit>,
    repo: Arc<ReadonlyRepo>,
    inodes: InodeTable,
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
            commit: Mutex::new(commit),
            repo,
            inodes: InodeTable::new(tree_id),
        })
    }
}

impl fractal_fuse::Filesystem for Fs {
    async fn lookup(&self, _req: Request, parent: Inode, name: &OsStr) -> FsResult<ReplyEntry> {
        let Ok(name) = str::from_utf8(name.as_bytes()) else {
            return Err(ENOENT);
        };
        let Ok(name) = RepoPathComponent::new(name) else {
            return Err(ENOENT);
        };
        let inode = self
            .inodes
            .get_or_insert_and_ref(&self.repo, parent, name)
            .await?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: self.inodes.stat(&self.repo, inode).await?,
            generation: 0,
        })
    }

    fn forget(&self, _: Request, inode: Inode, nlookup: u64) {
        self.inodes.forget(inode, nlookup);
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
            attr: self.inodes.stat(&self.repo, inode).await?,
        })
    }

    async fn open(&self, _req: Request, _inode: Inode, _flags: u32) -> FsResult<ReplyOpen> {
        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
            backing_id: 0,
        })
    }

    async fn read(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        self.inodes.read(&self.repo, inode, offset, buf).await
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
        let ((id, executable, copy_id), path) =
            self.inodes.get_file_id_path(inode).ok_or(EISDIR)?;
        let file = self.repo.store().read_file(&path, &id).await.map_err(|e| {
            error!("opening {path:?}: {:#}", e);
            EIO
        })?;
        trace!("opened old file {id}");
        // Create a new file with the specified data overwritten
        let updated_file = self
            .repo
            .store()
            .write_file(
                &path,
                &mut Overwrite {
                    inner: file,
                    offset,
                    data,
                },
            )
            .await
            .map_err(|e| {
                error!("writing {path:?}: {:#}", e);
                EIO
            })?;
        trace!("created edited file {updated_file}");

        let TreeValue::Tree(root_id) = self
            .inodes
            .inodes
            .read()
            .unwrap()
            .get(FUSE_ROOT_ID as usize)
            .unwrap()
            .value
            .clone()
        else {
            unreachable!()
        };
        let old_tree = self
            .repo
            .store()
            .get_tree(RepoPathBuf::root(), &root_id)
            .await
            .map_err(|e| {
                error!("fetching tree: {:#}", e);
                EIO
            })?;

        // TODO: Concurrent writes
        let mut inodes = self.inodes.inodes.write().unwrap();
        let new_tree = tree_insert(
            &mut *inodes,
            FUSE_ROOT_ID as usize,
            &old_tree,
            RepoPath::root(),
            &path,
            TreeValue::File {
                id: updated_file,
                executable,
                copy_id,
            },
        )
        .await
        .map_err(|e| {
            error!("building tree: {:#}", e);
            EIO
        })?;
        trace!("created edited tree {}", new_tree.id());

        // TODO: Concurrent writes
        let mut commit = self.commit.lock().unwrap();
        let mut tx = self.repo.start_transaction();
        *commit = tx
            .repo_mut()
            .rewrite_commit(&*commit)
            .set_tree(MergedTree::resolved(
                self.repo.store().clone(),
                new_tree.id().clone(),
            ))
            .write()
            .await
            .map_err(|e| {
                error!("committing write to {path:?}: {:#}", e);
                EIO
            })?;
        trace!("updated current commit to {}", commit.id());
        drop(commit);
        drop(inodes);
        tx.repo_mut().rebase_descendants().await.map_err(|e| {
            error!("rebasing descendants for write to {path:?}: {:#}", e);
            EIO
        })?;
        tx.commit("jj-fuse write").await.map_err(|e| {
            error!("committing write to {path:?}: {:#}", e);
            EIO
        })?;

        Ok(data.len())
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        self.inodes
            .readdir(&self.repo, inode, offset, size, |value, name, offset| {
                DirectoryEntry {
                    ino: 0,
                    offset,
                    kind: value_ty(value),
                    name: name.as_bytes().to_vec(),
                }
            })
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
        self.inodes
            .readdir(&self.repo, inode, offset, size, |value, name, offset| {
                DirectoryEntryPlus {
                    ino: 0,
                    offset,
                    kind: value_ty(value),
                    name: name.as_bytes().to_vec(),
                    entry_ttl: TTL,
                    attr: value_stat(value),
                    generation: 0,
                }
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

struct InodeTable {
    inodes: RwLock<Slab<InodeState>>,
}

impl InodeTable {
    fn new(root_id: TreeId) -> Self {
        let mut root_inode = InodeState::new(RepoPathBuf::root(), TreeValue::Tree(root_id.clone()));
        root_inode.children = Some(InodeChildren {
            nodes: Default::default(),
        });
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

    fn get_file_id_path(&self, inode: Inode) -> Option<((FileId, bool, CopyId), RepoPathBuf)> {
        let inodes = self.inodes.read().unwrap();
        let state = inodes.get(inode as usize).unwrap();
        Some((
            match &state.value {
                TreeValue::File {
                    id,
                    executable,
                    copy_id,
                } => (id.clone(), *executable, copy_id.clone()),
                _ => return None,
            },
            state.path.clone(),
        ))
    }

    async fn read(
        &self,
        repo: &ReadonlyRepo,
        inode: Inode,
        mut offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        let ((id, _, _), path) = self.get_file_id_path(inode).ok_or(EISDIR)?;
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
        let (path, value) = {
            let inodes = self.inodes.read().unwrap();
            let state = inodes.get(inode as usize).unwrap();
            (state.path.clone(), state.value.clone())
        };
        let mut size = 0;
        if let TreeValue::File { id, .. } = value {
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
            ..value_stat(
                &self
                    .inodes
                    .read()
                    .unwrap()
                    .get(inode as usize)
                    .unwrap()
                    .value,
            )
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
            let TreeValue::Tree(id) = &state.value else {
                return Err(ENOTDIR);
            };
            dir_path = state.path.clone();
            tree_id = id.clone();
            if offset < 1 {
                entries.push(f(&state.value, ".", 1));
            }
            if offset < 2
                && let Some(p) = &state.parent
            {
                entries.push(f(&inodes.get(p.parent).unwrap().value, "..", 2));
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
            entries.push(f(
                entry.value(),
                entry.name().to_fs_name().unwrap(),
                i as u64 + base + 1,
            ));
        }
        Ok(entries)
    }

    /// Returns `None` if not already resident
    fn get_and_ref(&self, parent: Inode, name: &RepoPathComponent) -> Option<Inode> {
        let inodes = self.inodes.read().unwrap();
        let i = *inodes
            .get(parent as usize)?
            .children
            .as_ref()?
            .nodes
            .read()
            .unwrap()
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

        let parent_tree_id;
        let parent_path;
        {
            let mut inodes = self.inodes.write().unwrap();
            // Guard against races
            if let Some(i) = inodes
                .get(parent_ino as usize)
                .unwrap()
                .children
                .as_ref()
                .and_then(|children| children.nodes.read().unwrap().get(name).copied())
            {
                inodes
                    .get(i)
                    .unwrap()
                    .references
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(i as u64);
            }

            // Create a child inode
            let parent = inodes.get_mut(parent_ino as usize).unwrap();
            let TreeValue::Tree(id) = &parent.value else {
                return Err(ENOTDIR);
            };
            parent_tree_id = id.clone();
            parent_path = parent.path.clone();
        }

        let child_path = parent_path.join(name);
        let parent_tree = repo
            .store()
            .get_tree(parent_path.clone(), &parent_tree_id)
            .await
            .map_err(|e| {
                error!("accessing parent directory {parent_path:?}: {:#}", e);
                EIO
            })?;
        let value = parent_tree.value(name).ok_or(ENOENT)?;
        let mut inode = InodeState::new(child_path, value.clone());
        inode.parent = Some(InodeParent {
            child_name: name.to_owned(),
            parent: parent_ino as usize,
        });
        if let TreeValue::Tree(_) = value {
            inode.children = Some(InodeChildren::new());
        }
        let mut inodes = self.inodes.write().unwrap();
        let n = inodes.insert(inode);
        trace!("{:?} is inode {}", inodes.get(n).unwrap().path, n);
        inodes
            .get_mut(parent_ino as usize)
            .unwrap()
            .children
            .as_mut()
            .unwrap()
            .nodes
            .write()
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
                .children
                .as_ref()
                .unwrap()
                .nodes
                .write()
                .unwrap()
                .remove(&*parent.child_name)
                .unwrap();
            debug_assert_eq!(parent_child, i);
        }
    }
}

struct InodeState {
    path: RepoPathBuf,
    value: TreeValue,
    references: AtomicU64,
    parent: Option<InodeParent>,
    /// Populated iff value is Tree
    children: Option<InodeChildren>,
}

impl InodeState {
    fn new(path: RepoPathBuf, value: TreeValue) -> Self {
        Self {
            path,
            value,
            references: AtomicU64::new(1),
            parent: None,
            children: None,
        }
    }
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
    let mut mode = 0o444;
    mode |= match value {
        TreeValue::File {
            executable: true, ..
        }
        | TreeValue::Tree(_) => 0o222,
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

struct InodeChildren {
    // Populated lazily
    nodes: RwLock<FxHashMap<RepoPathComponentBuf, usize>>,
}

impl InodeChildren {
    pub fn new() -> Self {
        Self {
            nodes: Default::default(),
        }
    }
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

async fn tree_insert(
    inodes: &mut Slab<InodeState>,
    parent_inode: usize,
    old: &Tree,
    path_so_far: &RepoPath,
    path_remaining: &RepoPath,
    value: TreeValue,
) -> BackendResult<Tree> {
    let mut iter = path_remaining.components();
    let first = iter.next().unwrap();
    let rest = iter.as_path();
    let old_entries = old.data().entries();
    let mut new_entries = Vec::new();
    for entry in old_entries {
        if entry.name() != first {
            // Untouched entry
            new_entries.push((entry.name().to_owned(), entry.value().clone()));
            continue;
        }
        let inode = *inodes
            .get(parent_inode)
            .unwrap()
            .children
            .as_ref()
            .unwrap()
            .nodes
            .read()
            .unwrap()
            .get(entry.name())
            .unwrap();
        if rest.as_internal_file_string().is_empty() {
            // Replaced entry
            new_entries.push((entry.name().to_owned(), value.clone()));
            trace!("updating {path_so_far:?}/{first:?} to {:?}", value);
            inodes.get_mut(inode).unwrap().value = value.clone();
            continue;
        }
        // Tree containing edited replaced at some depth
        let TreeValue::Tree(subtree) = entry.value() else {
            unreachable!();
        };
        let old_subtree = old
            .store()
            .get_tree(path_so_far.to_owned(), &subtree)
            .await?;
        let absolute_path = path_so_far.join(entry.name());
        let new_subtree = Box::pin(tree_insert(
            inodes,
            inode,
            &old_subtree,
            &absolute_path,
            rest,
            value.clone(),
        ))
        .await?;
        new_entries.push((
            entry.name().to_owned(),
            TreeValue::Tree(new_subtree.id().clone()),
        ));
    }
    let new_tree = jj_lib::backend::Tree::from_sorted_entries(new_entries);
    let new_tree = old.store().write_tree(path_so_far, new_tree).await?;
    trace!("updating tree at {path_so_far:?} to {}", new_tree.id());
    inodes.get_mut(parent_inode).unwrap().value = TreeValue::Tree(new_tree.id().clone());
    Ok(new_tree)
}

const TTL: Duration = Duration::from_secs(60);
