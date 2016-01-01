1.  Update version number in setup.py and libhdfs3/__init__.py.  Commit
2.  Tag git commit

        git tag -a 0.0.2 -m "Version 0.0.2"

3.  Push git commit and tag

        git push origin master --tags

4.  Upload to PyPI

        python setup.py register sdist bdist upload
