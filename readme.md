# Using DirectIO with Go

The purpose of this playground is to evaluate the optios of Nick Craig-Wood's [`directio`](https://github.com/ncw/directio) library.

## Features

This section describes the features implemented by both parties: Producer and Consumer.

### Configuration

The configuration items used by both parties are stored in `.env` file. You'll find there further details about each item's purpose.

### Producer

Accoring to the max file size (defined in `IO_FILE_MAX_SIZE` config item), Producer is:
- appending to the latest written file, if that file's size didn't reach the max size
- writing to a new file, if there is no written file or if the size of the latest one exceeded the max size

### Consumer

Consumer:
- reads (_consumes_) files - one by one - from the same path (define in `IO_PATH` config item)
- saves the state (aka `ConsumerState` in the consumer's code), so that it can resume the work any time

## Todos

- [ ] If writer fails to init properly, main should get notified, stop the producer, and end itself.
    - That can happens when the file system where the file resides does not support O_DIRECT flag.<br/>
      See these [notes on linux kernel and O_DIRECT](https://lists.archive.carbon60.com/linux/kernel/720702).

## Tests


### Reading Directory

Since I discovered that `ioutil.ReadDir` takes aprox 1.2 sec when there are +100K files in the directory, switched to using `os.File.Readdirnames`.
But that call is not listing the files in the order they were created or by file name. So the result must be sorted. But the overall exec time is considerably better than of `ioutil.ReadDir`'s one.

`read_dir_eval/readdir_eval.go` is a relevant example. Running it in a directory containing 499099 files, here are the figures (output):
```
>>> ioutil.ReadDir exec time: 1.311758808s
>>> os.File.Readdirnames exec time: 144.131977ms
>>> os.File.Readdirnames result has 499099 entries.
>>> sort exec time: 163.322515ms
```


For such a huge number of files, the standard `rm -f *.dat` does not work and the option is to use `find . -name "*.dat" -print0 | xargs -0 rm`.

