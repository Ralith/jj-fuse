use std::{
    ffi::CString,
    io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
};

use fractal_fuse::{FileAttr, Timestamp};
use libc::{O_DIRECTORY, O_PATH, PATH_MAX};

pub struct DirFd(OwnedFd);

impl DirFd {
    /// Open a directory by name
    pub fn open(path: &Path) -> io::Result<Self> {
        let path = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        unsafe {
            let fd = libc::open(path.as_ptr().cast(), O_DIRECTORY | O_PATH);
            if fd == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self(OwnedFd::from_raw_fd(fd)))
        }
    }

    /// Open something relative to this directory
    pub fn open_child(&self, child: &Path) -> io::Result<OwnedFd> {
        let path = CString::new(child.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        unsafe {
            let fd = libc::openat(self.0.as_raw_fd(), path.as_ptr().cast(), 0);
            if fd == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(OwnedFd::from_raw_fd(fd))
        }
    }

    pub fn stat_child(&self, child: &Path) -> io::Result<FileAttr> {
        let path = CString::new(child.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        unsafe {
            let mut stat = mem::zeroed();
            let r = libc::fstatat64(self.0.as_raw_fd(), path.as_ptr().cast(), &mut stat, 0);
            if r == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(FileAttr {
                ino: 0,
                size: stat.st_size as u64,
                blocks: stat.st_blocks as u64,
                atime: Timestamp {
                    sec: stat.st_atime as u64,
                    nsec: stat.st_atime_nsec as u32,
                },
                mtime: Timestamp {
                    sec: stat.st_mtime as u64,
                    nsec: stat.st_mtime_nsec as u32,
                },
                ctime: Timestamp {
                    sec: stat.st_ctime as u64,
                    nsec: stat.st_ctime_nsec as u32,
                },
                mode: stat.st_mode,
                nlink: stat.st_nlink as u32,
                uid: stat.st_uid,
                gid: stat.st_gid,
                rdev: stat.st_rdev as u32,
                blksize: stat.st_blksize as u32,
            })
        }
    }

    pub fn readlink_child(&self, child: &Path) -> io::Result<Vec<u8>> {
        let path = CString::new(child.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let mut buf = [0u8; PATH_MAX as usize];
        unsafe {
            let r = libc::readlinkat(
                self.0.as_raw_fd(),
                path.as_ptr().cast(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            );
            if r == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(buf[0..r as usize].to_vec())
        }
    }
}
