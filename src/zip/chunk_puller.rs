use core::cmp::min;

use crate::{ChunkPuller, ConcurrentIter};

pub struct ZippedChunkPuller<P1, P2>
where
    P1: ChunkPuller,
    P2: ChunkPuller,
{
    left: P1,
    right: P2,
    index: usize,
    chunk_size: usize,
}

impl<'c, P1, P2> ZippedChunkPuller<P1, P2>
where
    P1: ChunkPuller,
    P2: ChunkPuller,
{
    pub fn new<'i, I1, I2>(left: &'i I1, right: &'i I2, chunk_size: usize) -> Self
    where
        I1: ConcurrentIter<ChunkPuller<'i> = P1>,
        I2: ConcurrentIter<ChunkPuller<'i> = P2>,
    {
        Self {
            left: left.chunk_puller(chunk_size),
            right: right.chunk_puller(chunk_size),
            chunk_size,
            index: 0,
        }
    }
}

impl<P1: ChunkPuller, P2: ChunkPuller> ChunkPuller for ZippedChunkPuller<P1, P2> {
    type ChunkItem = (P1::ChunkItem, P2::ChunkItem);

    type Chunk<'c>
        = ZippedChunk<P1::Chunk<'c>, P2::Chunk<'c>>
    where
        Self: 'c;

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pull(&mut self) -> Option<Self::Chunk<'_>> {
        let left = self.left.pull()?;
        let right = self.right.pull()?;
        let chunk_len = min(left.len(), right.len());
        let output = Some(ZippedChunk {
            left,
            right,
            index: self.index,
        });
        self.index += chunk_len;
        output
    }

    fn pull_with_idx(&mut self) -> Option<(usize, Self::Chunk<'_>)> {
        let (_, left) = self.left.pull_with_idx()?;
        let (_, right) = self.right.pull_with_idx()?;
        let chunk_len = min(left.len(), right.len());
        let output = Some((
            self.index,
            ZippedChunk {
                left,
                right,
                index: self.index,
            },
        ));
        self.index += chunk_len;
        output
    }
}

pub struct ZippedChunk<C1, C2>
where
    C1: ExactSizeIterator,
    C2: ExactSizeIterator,
{
    left: C1,
    right: C2,
    index: usize,
}

impl<C1: ExactSizeIterator, C2: ExactSizeIterator> Iterator for ZippedChunk<C1, C2> {
    type Item = (C1::Item, C2::Item);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < min(self.left.len(), self.right.len()) {
            // Don't want to pull from the longer iterator first if the shorter iterator has been exhausted
            let tuple = (self.left.next()?, self.right.next()?);
            self.index += 1;
            Some(tuple)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = min(self.left.len(), self.right.len());

        (len, Some(len))
    }
}

impl<C1: ExactSizeIterator, C2: ExactSizeIterator> ExactSizeIterator for ZippedChunk<C1, C2> {}
