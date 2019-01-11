# AIS Python Scripts

This repo is just a collection of my Python scrips written to work with AIS data.

Each can be loaded into Python directly from GitHub using the code below:

```python
import requests
def githubimport(user, repo, module):
   d = {}
   url = 'https://raw.githubusercontent.com/{}/{}/master/{}.py'.format(user, repo, module)
   r = requests.get(url).text
   exec(r, d)
   return d

log_to_json = githubimport('cjcallag', 'ais', 'log_to_json')
# You should change the object name to match the module
.
```

Once loaded, you may want to take a look at the script to understand the required arguments and my logic (or lack thereof).
