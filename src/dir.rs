use std::{
    ffi::CStr,
    io,
    os::fd::{IntoRawFd, OwnedFd},
    ptr::NonNull,
};

use fractal_fuse::FileType;
use libc::{DT_BLK, DT_CHR, DT_DIR, DT_FIFO, DT_LNK, DT_REG, DT_SOCK};

pub struct Dir(NonNull<libc::DIR>);

impl Dir {
    pub fn new(fd: OwnedFd) -> io::Result<Self> {
        let dir = unsafe { libc::fdopendir(fd.into_raw_fd()) };
        Ok(Self(
            NonNull::new(dir).ok_or_else(io::Error::last_os_error)?,
        ))
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0.as_ptr());
        }
    }
}

impl Iterator for Dir {
    type Item = io::Result<DirEnt>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            unsafe {
                *libc::__errno_location() = 0;
                let dirent = libc::readdir64(self.0.as_ptr());
                if dirent.is_null() {
                    let err = io::Error::last_os_error();
                    if err.raw_os_error() == Some(0) {
                        return None;
                    } else {
                        return Some(Err(err));
                    }
                }
                return Some(Ok(DirEnt {
                    name: CStr::from_ptr((*dirent).d_name.as_ptr().cast())
                        .to_bytes()
                        .to_vec(),
                    ty: match (*dirent).d_type {
                        DT_BLK => FileType::BlockDevice,
                        DT_CHR => FileType::CharDevice,
                        DT_DIR => FileType::Directory,
                        DT_FIFO => FileType::NamedPipe,
                        DT_LNK => FileType::Symlink,
                        DT_REG => FileType::RegularFile,
                        DT_SOCK => FileType::RegularFile,
                        _ => continue,
                    },
                }));
            }
        }
    }
}

pub struct DirEnt {
    pub name: Vec<u8>,
    pub ty: FileType,
}
