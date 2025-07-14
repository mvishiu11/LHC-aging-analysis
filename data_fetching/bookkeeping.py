import requests, itertools, time
BK_URL = "https://ali-bookkeeping.cern.ch/api/runs"
PAR  = dict(filter={'detectors[operator]':'and',
                    'detectors[values]':'FT0',
                    'runTypes[]':'5',          # LASER
                    'runQualities':'good'})
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6Ijg5MDYzNCIsInVzZXJuYW1lIjoiamFrdWJtaWwiLCJuYW1lIjoiSmFrdWIgTWlsb3N6IE11c3p5bnNraSIsImFjY2VzcyI6Imd1ZXN0LGRlZmF1bHQtcm9sZSIsImlhdCI6MTc1MjIyNTQ5NiwiZXhwIjoxNzUyODMwMjk2LCJpc3MiOiJvMi11aSJ9.NyiBr6FyIyJ20dz9rEcgVYx1rcY1oPo3gUhhW_iXuzI"
runs  = set()
for offset in itertools.count(step:=600):
    resp = requests.get(BK_URL,
                        params={**PAR, 'page[offset]': offset,
                                               'page[limit]': step},
                        headers={'Authorization': f'Bearer {TOKEN}'})
    data = resp.json()
    runs |= {d['runNumber'] for d in data['data']}
    if offset//step+1 >= data['meta']['page']['pageCount']:
        break
