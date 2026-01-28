use std::fmt;
use std::mem::MaybeUninit;

/// A wrapper around a byte buffer that is incrementally filled and tracks
/// initialization progress.
pub struct ReadBuf<'a> {
    buf: &'a mut [MaybeUninit<u8>],
    filled: usize,
    initialized: usize,
}

impl<'a> ReadBuf<'a> {
    /// Create a new `ReadBuf` from a fully initialized buffer.
    #[inline]
    pub fn new(buf: &'a mut [u8]) -> Self {
        let len = buf.len();
        // Safety: &[u8] has the same layout as &[MaybeUninit<u8>], and
        // [u8] is always initialized.
        let buf = unsafe { &mut *(buf as *mut [u8] as *mut [MaybeUninit<u8>]) };
        Self {
            buf,
            filled: 0,
            initialized: len,
        }
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Returns a shared reference to the filled portion of the buffer.
    #[inline]
    pub fn filled(&self) -> &[u8] {
        // Safety: filled bytes are always initialized
        unsafe {
            &*(self.buf.get_unchecked(..self.filled) as *const [MaybeUninit<u8>] as *const [u8])
        }
    }

    /// Returns the number of bytes at the end of the slice that are unfilled.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity() - self.filled
    }
    /// Append data to the buffer.
    ///
    /// Advances both the initialized and filled cursors.
    #[inline]
    pub fn put_slice(&mut self, data: &[u8]) {
        assert!(self.remaining() >= data.len(), "not enough space in buffer");

        // Copy data into the unfilled portion
        let dest = &mut self.buf[self.filled..self.filled + data.len()];
        // Safety: we're writing initialized data
        for (d, s) in dest.iter_mut().zip(data) {
            d.write(*s);
        }

        // Update cursors
        let new_filled = self.filled + data.len();
        self.initialized = self.initialized.max(new_filled);
        self.filled = new_filled;
    }
}

impl fmt::Debug for ReadBuf<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBuf")
            .field("capacity", &self.capacity())
            .field("filled", &self.filled)
            .field("initialized", &self.initialized)
            .finish()
    }
}
