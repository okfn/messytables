# How to contribute to messytables

If you make a change in messytables, you should:

* Write tests for your code
* Make sure all tests are passing
* Add a note to the `CHANGELOG.md`
* Send a pull request and have someone reviewing it

For small changes and doc changes, it is not necessary to file a pull request.

## Running the tests

```bash
source pyenv/messytables/bin/activate
pip install -r requirements-test.txt
nosetests
```
