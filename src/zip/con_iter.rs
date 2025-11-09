use core::cmp::min;

use parking_lot::Mutex;

use crate::{ConcurrentIter, zip::chunk_puller::ZippedChunkPuller};

/// Concurrent zip iterator
pub struct ConIterZip<I1: ConcurrentIter, I2: ConcurrentIter> {
    left: I1,
    right: I2,
    index: Mutex<usize>,
}

impl<I1, I2> ConIterZip<I1, I2>
where
    I1: ConcurrentIter,
    I2: ConcurrentIter,
{
    /// Creates a new ConIterZip
    pub fn new(left: I1, right: I2) -> Self {
        Self {
            left,
            right,
            index: Mutex::new(0),
        }
    }
}

impl<I1: ConcurrentIter, I2: ConcurrentIter> ConcurrentIter for ConIterZip<I1, I2> {
    type Item = (I1::Item, I2::Item);

    type SequentialIter = core::iter::Zip<I1::SequentialIter, I2::SequentialIter>;

    type ChunkPuller<'c>
        = ZippedChunkPuller<I1::ChunkPuller<'c>, I2::ChunkPuller<'c>>
    where
        Self: 'c;

    fn into_seq_iter(self) -> Self::SequentialIter {
        self.left.into_seq_iter().zip(self.right.into_seq_iter())
    }

    fn skip_to_end(&self) {
        self.left.skip_to_end();
        self.right.skip_to_end();
    }

    fn next(&self) -> Option<Self::Item> {
        let mut index = self.index.lock();
        let output = Some((self.left.next()?, self.right.next()?));
        *index += 1;
        output
    }

    fn next_with_idx(&self) -> Option<(usize, Self::Item)> {
        let mut index = self.index.lock();
        let output = Some((*index, (self.left.next()?, self.right.next()?)));
        *index += 1;
        output
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, min(self.left.size_hint().1, self.right.size_hint().1))
    }

    fn is_completed_when_none_returned(&self) -> bool {
        self.left.is_completed_when_none_returned() && self.right.is_completed_when_none_returned()
    }

    fn chunk_puller(&self, chunk_size: usize) -> Self::ChunkPuller<'_> {
        ZippedChunkPuller::new(&self.left, &self.right, chunk_size)
    }
}
