# Resolve LFS pointers in EROFS snapshots

## Goal

An EROFS image should contain the bytes a checkout reads for a regular Git
LFS file. The Git tree contains only a small pointer. The snapshot builder
must use the pointer's object size and stream the corresponding LFS blob.

## Plan

1. Rebase the LFS branch on `origin/main`, which introduced snapshots.
2. Recognize canonical v1 pointers in regular Git blobs smaller than 1024
   bytes. Leave ordinary blobs alone. Fail on malformed pointers and pointers
   with extensions, since extensions require a client-side smudge filter.
3. Open the shared blob only after checking the repository's membership marker
   and its size against the pointer. A missing or mismatched object fails the
   image build. The push itself still succeeds.
4. Advance the image format to v2. The v1 images can contain pointer text,
   so a builder must not treat one as a completed v2 image.
5. Test image contents, permissions, missing objects, marker isolation, and
   the Smart HTTP push path with an in-memory bucket.

The pointer size and canonical line format follow the
[Git LFS specification](https://github.com/git-lfs/git-lfs/blob/main/docs/spec.md).
