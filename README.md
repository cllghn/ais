# AIS Python Scripts

This repo is just a collection of my Python scrips written to work with AIS data.

```python
import requests
def githubimport(user, repo, module):
   d = {}
   url = 'https://raw.githubusercontent.com/{}/{}/master/{}.py'.format(user, repo, module)
   r = requests.get(url).text
   exec(r, d)
   return d

ais_extractor = githubimport('cjcallag', 'ais', 'ais_extractor')
```
