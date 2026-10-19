# Google News 検索RSSを取得して leak-live Worker の /ingest に送る。
# 取得計画（語×窓）は Worker の /plan が決める。100件上限（split_at 以上）に当たった窓は「語 X」「語 -X」に二分して取り直す（和集合＝元の検索結果）。
import json,os,sys,time,re,urllib.request,urllib.parse,concurrent.futures as cf
W="https://leak-live.ueda-855.workers.dev"
UA={"User-Agent":"stak-leak-gn-fetch/2.0"}
kind=(os.environ.get("KIND") or "full").strip()
plan=json.load(open(os.environ["PLAN_FILE"])) if os.environ.get("PLAN_FILE") else json.load(urllib.request.urlopen(urllib.request.Request(W+"/plan?kind="+kind,headers=UA),timeout=20))
def url(q,w): return "https://news.google.com/rss/search?q="+urllib.parse.quote(q+" when:"+w)+"&hl=ja&gl=JP&ceid=JP:ja"
def get(u):
  err=None
  for a in range(4):
    try:
      x=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"stak-leak-bot/0.2"}),timeout=20).read().decode()
      if "<rss" in x or "<channel" in x: return x,None
      err="not rss"
    except Exception as e: err=str(e)
    time.sleep(2*(a+1))
  return None,err
results=[];errors=[]
def run(q,w,depth,maxd,mind=0):
  u=url(q,w);x,e=get(u)
  if x is None: errors.append({"url":u,"q":q,"w":w,"err":e}); return
  n=len(re.findall(r"<item>",x));r={"q":q,"w":w,"url":u,"xml":x,"n":n,"depth":depth,"split":False}
  results.append(r)
  if (n>=plan["split_at"] and depth<maxd) or depth<mind:
    used=set(re.sub(r"^-","",t) for t in q.split()[1:])
    terms=[t for t in plan["split_terms"] if t not in used and t not in q]
    if terms:
      r["split"]=True;t=terms[0]
      run(q+" "+t,w,depth+1,maxd,mind); run(q+" -"+t,w,depth+1,maxd,mind)
t0=time.time()
with cf.ThreadPoolExecutor(3) as ex:
  list(ex.map(lambda p:run(p["q"],p["w"],0,p["max_depth"],p.get("min_depth",0)),plan["queries"]))
body={"v":2,"kind":kind,"event":os.environ.get("EVENT",""),"run_id":os.environ.get("RUN_ID",""),"results":results,"errors":errors}
if os.environ.get("DRY"):
  json.dump(body,open(os.environ["DRY"],"w"),ensure_ascii=False);print("dry",len(results),"results",len(errors),"errors",round(time.time()-t0,1),"s",[(r["q"],r["w"],r["n"]) for r in results if r["n"]>=90 or r["depth"]]);sys.exit(0)
req=urllib.request.Request(W+"/ingest",data=json.dumps(body).encode(),headers={**UA,"X-Ingest-Key":os.environ["INGEST_KEY"],"Content-Type":"application/json"},method="POST")
res=json.load(urllib.request.urlopen(req,timeout=60))
print(json.dumps({k:res.get(k) for k in ("kind","results","rejected","unique","added","store_total","max_n","unsplit_capped")},ensure_ascii=False))
print("fetch_s",round(time.time()-t0,1),"splits",sum(1 for r in results if r["split"]),"errors",len(errors))
for e in errors: print("ERR",e["q"],e["w"],e["err"])
base_fail=[e for e in errors if e["q"] in {p["q"] for p in plan["queries"]}]
if base_fail or res.get("rejected"): sys.exit("incomplete")
