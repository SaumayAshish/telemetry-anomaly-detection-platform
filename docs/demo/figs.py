import pandas as pd, numpy as np, json
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

SURF='#fcfcfb'; INK='#0b0b0b'; INK2='#52514e'; MUTED='#898781'; GRID='#e1e0d9'; AXIS='#c3c2b7'
BLUE='#2a78d6'; ORANGE='#eb6834'
SEV={'LOW':'#86b6ef','MEDIUM':'#3987e5','HIGH':'#1c5cab','CRITICAL':'#0d366b'}
SEVSIZE={'LOW':26,'MEDIUM':40,'HIGH':60,'CRITICAL':84}
plt.rcParams.update({'font.family':['DejaVu Sans'],'axes.facecolor':SURF,'figure.facecolor':SURF,
  'axes.edgecolor':AXIS,'axes.labelcolor':INK2,'xtick.color':MUTED,'ytick.color':MUTED,'text.color':INK,
  'axes.spines.top':False,'axes.spines.right':False,'axes.linewidth':1,'grid.color':GRID,'grid.linewidth':1})

tr=pd.read_csv('trace.csv',parse_dates=['ts']); tr['ts']=pd.to_datetime(tr.ts,utc=True)
al=pd.read_csv('alerts.csv'); al['ts']=pd.to_datetime(al.reading_timestamp,utc=True)
gt=pd.read_csv('ground-truth.csv'); gt['from']=pd.to_datetime(gt['from'],utc=True); gt['to']=pd.to_datetime(gt['to'],utc=True)
t0=tr.ts.min()
sec=lambda s:(s-t0).dt.total_seconds()
tr['t']=sec(tr.ts); al['t']=sec(al.ts); gt['t0']=sec(gt['from']); gt['t1']=sec(gt['to'])+0.5
sc=pd.read_csv('scorecard.csv')
LABEL={'SPIKE':'spike','LEVEL_SHIFT':'level shift','DRIFT':'drift'}
sensors=sorted(tr.sensor.unique())

# ---------- Figure 1: hero ----------
fig,axes=plt.subplots(5,1,figsize=(12,10.5),sharex=True)
for ax,s in zip(axes,sensors):
    d=tr[tr.sensor==s]
    ax.grid(axis='y'); ax.set_axisbelow(True)
    for _,f in gt[gt.sensor_id==s].iterrows():
        w0,w1=f.t0,max(f.t1,f.t0+2.5)
        ax.axvspan(w0,w1,color=ORANGE,alpha=0.14,lw=0)
        miss=not sc[(sc.sensor==s)&(sc.type==f.fault_type)&(sc.sigmas==f.magnitude_sigmas)].detected.iloc[0]
        txt=f"{LABEL[f.fault_type]} {f.magnitude_sigmas:+g}σ"+("  · missed" if miss else "")
        ax.text((w0+w1)/2,1.02,txt,transform=ax.get_xaxis_transform(),ha='center',va='bottom',fontsize=8.5,color=INK2)
    ax.plot(d.t,d.value,color=MUTED,lw=0.9,solid_capstyle='round',zorder=2)
    a=al[al.sensor_id==s]
    for sv in ['LOW','MEDIUM','HIGH','CRITICAL']:
        x=a[a.severity==sv]
        ax.scatter(x.t,x.value,s=SEVSIZE[sv],color=SEV[sv],edgecolor=SURF,linewidth=1.5,zorder=3)
    ax.set_ylabel(s,rotation=0,ha='right',va='center',fontsize=10,color=INK2,labelpad=10)
    ax.tick_params(labelsize=8.5)
axes[-1].set_xlabel('seconds since scenario start',fontsize=9.5)
axes[-1].set_xlim(0,300)
fig.suptitle('Five sensors, ten injected faults: 9 detected, 1 missed',x=0.065,y=0.985,ha='left',fontsize=15,fontweight='bold')
fig.text(0.065,0.953,'Readings (grey), injected fault windows (orange wash), alerts raised (dots, sized and shaded by severity). Seed 42, 600 ticks per sensor at 500 ms.',fontsize=9.5,color=INK2,ha='left')
h=[Patch(facecolor=ORANGE,alpha=0.25,label='injected fault')]+[Line2D([],[],marker='o',ls='',color=SEV[k],markeredgecolor=SURF,markersize=np.sqrt(SEVSIZE[k])*1.0,label=k.capitalize()) for k in SEV]
fig.legend(handles=h,loc='lower center',ncol=5,frameon=False,fontsize=9,bbox_to_anchor=(0.5,0.0),labelcolor=INK2)
fig.subplots_adjust(left=0.085,right=0.985,top=0.9,bottom=0.075,hspace=0.55)
fig.savefig('images/detection-overview.png',dpi=170); plt.close(fig)

# ---------- Figure 2: scorecard ----------
sc['name']=sc.sensor+'  '+sc.type.map(LABEL)+'  '+sc.sigmas.map(lambda v:f'{v:+g}σ')
fig,ax=plt.subplots(figsize=(10,5.2))
y=np.arange(len(sc))[::-1]
ax.grid(axis='x'); ax.set_axisbelow(True)
ax.barh(y,sc.alerts,height=0.5,color=BLUE)
for yi,(_,r) in zip(y,sc.iterrows()):
    if r.detected:
        d='first alert on the fault\'s first reading' if r.delay_s==0 else f'first alert {r.delay_s:g} s into the fault'
        ax.text(r.alerts+0.35,yi,f'{int(r.alerts)}  ·  {d}',va='center',fontsize=9,color=INK2)
    else:
        ax.text(0.35,yi,'missed: 2.5σ is below the 3σ threshold',va='center',fontsize=9,color=INK2)
ax.set_yticks(y); ax.set_yticklabels(sc.name,fontsize=9.5,color=INK)
ax.set_xlim(0,34); ax.set_xlabel('alerts raised inside the fault window',fontsize=9.5)
ax.tick_params(axis='y',length=0); ax.tick_params(axis='x',labelsize=8.5)
fig.suptitle('Detection scorecard per injected fault',x=0.02,y=0.975,ha='left',fontsize=14,fontweight='bold')
fig.text(0.02,0.918,'Level shifts stop at exactly 10 alerts (the rebaseline rule). Drifts are caught only once the ramp passes about 3σ.',fontsize=9.5,color=INK2)
fig.subplots_adjust(left=0.25,right=0.97,top=0.86,bottom=0.12)
fig.savefig('images/detection-scorecard.png',dpi=170); plt.close(fig)

# ---------- Figure 3: z-score trace S-1001 ----------
d=tr[tr.sensor=='S-1001'].copy(); a=al[al.sensor_id=='S-1001']
fig,ax=plt.subplots(figsize=(12,4.6))
ax.grid(axis='y'); ax.set_axisbelow(True)
for _,f in gt[gt.sensor_id=='S-1001'].iterrows():
    ax.axvspan(f.t0,max(f.t1,f.t0+2.5),color=ORANGE,alpha=0.14,lw=0)
ax.axhspan(-3,3,color=BLUE,alpha=0.06,lw=0)
for yv in (3,-3): ax.axhline(yv,color=AXIS,lw=1)
ax.plot(d.t,d.z,color=MUTED,lw=0.9)
for sv in ['LOW','MEDIUM','HIGH','CRITICAL']:
    x=a[a.severity==sv]
    ax.scatter(x.t,x.z_score,s=SEVSIZE[sv],color=SEV[sv],edgecolor=SURF,linewidth=1.5,zorder=3)
ax.text(2,3.3,'+3σ threshold',fontsize=8.5,color=INK2); ax.text(2,-3.9,'−3σ threshold',fontsize=8.5,color=INK2); ax.set_ylim(-7,11)
ax.annotate('spike +12σ\nmeasured z = 10.1',xy=(40,10.1),xytext=(52,9.5),fontsize=9,color=INK2,arrowprops=dict(arrowstyle='-',color=AXIS,lw=1))
ax.annotate('level shift +6σ: 10 alerts,\nthen the baseline resets',xy=(129,6.5),xytext=(142,7.0),fontsize=9,color=INK2,arrowprops=dict(arrowstyle='-',color=AXIS,lw=1))
ax.annotate('scoring pauses for 30 readings\nwhile the new baseline warms up',xy=(137,0.3),xytext=(150,-5.6),fontsize=9,color=INK2,arrowprops=dict(arrowstyle='-',color=AXIS,lw=1))
ax.set_xlim(0,300); ax.set_xlabel('seconds since scenario start',fontsize=9.5); ax.set_ylabel('z-score vs. rolling baseline',fontsize=9.5)
ax.tick_params(labelsize=8.5)
fig.suptitle('Inside the detector: sensor S-1001',x=0.045,y=0.975,ha='left',fontsize=14,fontweight='bold')
fig.text(0.045,0.905,'Z-score of every scored reading. Alerts are the dots above ±3σ; orange marks the injected faults; the sparse early dots are ordinary noise.',fontsize=9.5,color=INK2)
fig.subplots_adjust(left=0.07,right=0.985,top=0.84,bottom=0.14)
fig.savefig('images/zscore-trace-s1001.png',dpi=170); plt.close(fig)
print(d[(d.t>120)&(d.t<140)][['t','z','scored','anomaly']].head(30).to_string())
print(a[['t','z_score','severity']].to_string())
