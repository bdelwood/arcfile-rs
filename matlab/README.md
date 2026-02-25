# Matlab bindings for `arcfile-rs`

## Test

```matlab
readarc_rs
utc = readarc_rs('bad', 'array.frame.utc')
utc = readarc_rs('../20230601_001102.dat.gz', 'array.frame.utc')
utc = readarc_rs('../20230601_001102.dat.gz', 'bad')
```
