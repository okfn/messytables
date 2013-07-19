# How to contribute to messytables

## Contributing a feature

You are very welcome to submit code features or doc changes for review and merge into the central git repository:

* Create an issue
* Create a git branch with the format `ISSUE_NUMBER-brief-one-line-synopsis-of-the-work`
* Commit and push your changes and write tests for your code
* Use [PEP8](http://www.python.org/dev/peps/pep-0008/), where possible
* Make sure all tests are passing
* Add a note to the `CHANGELOG.md`
* Add new requirements to `setup.py`
* Send a pull request and have someone to review it

For small changes and doc changes, it is not necessary to file a pull request.


## Running the tests

```bash
source pyenv/messytables/bin/activate
pip install -r requirements-test.txt
nosetests
```

## Merging a pull request

If you're reviewing a pull request for messytables, when merging a branch into master:

* Use the `--no-ff` option in the `git merge` command


## Making a release

If you want to release a new version:

* Make sure everything works as expected and all features are reasonably stable
* Tag the HEAD using [semantic versioning](http://semver.org/) and push them (`git push --tags`)
* Push to PyPi
