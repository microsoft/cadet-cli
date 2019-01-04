# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Getting started

+ Install [python >= 3.7.2](https://www.python.org/downloads/)
+ Ensure you have [pip](https://pip.pypa.io/en/stable/installing/)
+ `cd src`
+ `pip install -r requirements.txt`
+ Ready to code and run

## Dependency Management

In this project, we manage dependencies [this way](https://www.kennethreitz.org/essays/a-better-pip-workflow) (because it's simple and meets our minimum requirements).

To add dependencies:

+ Add an entry to `requirements-to-freeze.txt`
+ Then run `pip install -r requirements-to-freeze.txt`
+ Finally, when ready to lock versions (before release), run `pip freeze > requirements.txt`

To update dependencies:

+ Run `pip install -r requirements-to-freeze.txt --upgrade`
+ Finally, when ready to lock versions (before release), run `pip freeze > requirements.txt`