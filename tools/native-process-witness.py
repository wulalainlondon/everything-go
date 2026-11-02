#!/usr/bin/env python3
"""Bounded read-only Darwin exit witness for a reviewed maintenance plan.

Never signals observed processes. PID/start/exe/parent/group are fenced before
and after kqueue registration. A connection close is not a process exit. The
kernel wait status never identifies who initiated a foreign process's exit.
"""
import argparse, datetime, json, os, select, subprocess, time
from contextlib import closing
from pathlib import Path

# Darwin SDK sys/event.h NOTE_EXITSTATUS / NOTE_EXIT_DETAIL. EXITSTATUS is
# available for a child, or a same-user target the observer may signal. This
# observer only reads kevents; permission refusal is explicit, never bypassed.
EXIT_STATUS = 0x04000000
EXIT_DETAIL = 0x02000000

def processes():
    raw = subprocess.check_output(['ps','-axo','pid=,ppid=,pgid=,lstart=,comm='],env={**os.environ,'LC_ALL':'C'},text=True)
    rows = {}
    for row in raw.splitlines():
        p = row.strip().split(None,8)
        if len(p)==9:
            rows[int(p[0])]={'ppid':int(p[1]),'pgid':int(p[2]),'start':' '.join(p[3:8]),'exe':p[8]}
    return rows

def publish(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    nxt=path.with_name(path.name+'.next')
    with open(nxt,'x') as f:
        os.chmod(nxt,0o600);json.dump(value,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
    os.replace(nxt,path)
    fd=os.open(path.parent,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)

def register(queue, expected):
    before=processes();admitted={};refusals={}
    for key, identity in expected.items():
        pid=int(key)
        if before.get(pid)!=identity:
            refusals[key]='original_identity_not_current';continue
        try:
            queue.control([select.kevent(pid,filter=select.KQ_FILTER_PROC,flags=select.KQ_EV_ADD|select.KQ_EV_ONESHOT,
                                         fflags=select.KQ_NOTE_EXIT|EXIT_STATUS|EXIT_DETAIL)],0,0)
        except OSError as e:
            refusals[key]='kernel_registration_refused_'+str(e.errno);continue
        if processes().get(pid)!=identity:
            try: queue.control([select.kevent(pid,filter=select.KQ_FILTER_PROC,flags=select.KQ_EV_DELETE)],0,0)
            except OSError: pass
            refusals[key]='identity_changed_during_registration';continue
        admitted[key]=identity
    return admitted,refusals

def exit_event(event):
    result={'pid':int(event.ident),'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'flags':int(event.fflags),'initiator':'unknown','scope':'kernel_exit_observation_not_caller_authorization'}
    if event.flags & select.KQ_EV_ERROR:
        return {**result,'state':'observation_error','error_number':int(event.data)}
    if not event.fflags & select.KQ_NOTE_EXIT:
        return {**result,'state':'unqualified_kernel_event'}
    result['state']='exit_observed'
    if event.fflags & EXIT_STATUS:
        status=int(event.data);result['wait_status']=status
        if os.WIFEXITED(status): result['exit_code']=os.WEXITSTATUS(status)
        elif os.WIFSIGNALED(status): result['signal']=os.WTERMSIG(status)
    else: result['status_availability']='kernel_did_not_supply_exitstatus'
    return result

def watch(plan, output, ready, duration, stop):
    expected=json.loads(Path(plan).read_text())['protected']
    if not expected or len(expected)>128: raise RuntimeError('bounded_original_protected_set_required')
    if Path(output).exists() or Path(ready).exists(): raise RuntimeError('original_witness_output_already_exists_no_replay')
    with closing(select.kqueue()) as queue:
        admitted,refusals=register(queue,expected)
        record={'state':'observing' if not refusals else 'registration_incomplete','originalPlan':str(plan),
                'startedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'observerPID':os.getpid(),
                'registered':admitted,'refusals':refusals,'events':[],'signalsSent':False}
        publish(output,record);publish(ready,{'ready':not refusals,'output':str(output),'observerPID':os.getpid()})
        if refusals: return
        deadline=time.monotonic()+duration
        while time.monotonic()<deadline and not Path(stop).exists():
            for event in queue.control(None,128,min(1,max(0,deadline-time.monotonic()))):
                if str(event.ident) in admitted: record['events'].append(exit_event(event))
            if len(record['events'])>128: raise RuntimeError('witness_event_bound_exceeded')
            publish(output,record)
        record['state']='stopped_by_original_controller' if Path(stop).exists() else 'bounded_window_ended'
        record['endedAt']=datetime.datetime.now(datetime.timezone.utc).isoformat();publish(output,record)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['plan','output','ready','stop']:p.add_argument('--'+name,required=True,type=Path)
    p.add_argument('--duration',type=int,default=300);a=p.parse_args()
    if not 1<=a.duration<=600:raise SystemExit('duration_out_of_bounds')
    watch(a.plan,a.output,a.ready,a.duration,a.stop)
