# package

This folder contains the utilities needed to package `cadet` into an install-able format, using [pyInstaller](https://pyinstaller.readthedocs.io/).

## Producing a package

> Note: to create builds for different operating systems, these steps must be done on each desired os.

The following commands will produce a build artifact in the `./dist` folder.

```
# note: ensure you're in the /package directory of this project
pip install -r requirements.txt
pyinstaller --onefile ../src/cadet.py
```