"""New, fail-closed RAM credential destination. No old adapter imports/calls."""
import os, pathlib, plistlib, re, subprocess, uuid, stat

class RamDestinationError(RuntimeError): pass

def run(args):
    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode: raise RamDestinationError('owned RAM operation failed: '+args[0]+' exit '+str(p.returncode))
    return p.stdout

def image_backing(device):
    images = plistlib.loads(run(['/usr/bin/hdiutil','info','-plist']))['images']
    matching = [i for i in images if any(e.get('dev-entry') == device for e in i.get('system-entities', []))]
    if len(matching) != 1: raise RamDestinationError('owned RAM image binding missing')
    return matching[0].get('image-path')

class RamKeyVolume:
    sectors = 65536
    def __init__(self):
        self.device = None; self.mount = None; self.folder = None; self.key_path = None
        self.label = 'BridgeNotaryRAM-' + uuid.uuid4().hex[:12]
        self.dir_fd = None; self.key_fd = None; self.receipt = {}; self.cleaned = False
        self.key_identity = None
    def __enter__(self):
        try:
            output = run(['/usr/bin/hdiutil','attach','-nomount','ram://'+str(self.sectors)]).decode()
            devices = re.findall(r'^(/dev/disk\d+)\s', output, re.M)
            if len(devices) != 1: raise RamDestinationError('RAM allocator did not return one whole owned device')
            self.device = devices[0]
            backing = image_backing(self.device)
            if backing != 'ram://'+str(self.sectors): raise RamDestinationError('allocator is not expected RAM backing')
            info = plistlib.loads(run(['/usr/sbin/diskutil','info','-plist',self.device]))
            if info.get('BusProtocol') != 'Disk Image' or info.get('Internal') is not False or info.get('TotalSize') != self.sectors*512:
                raise RamDestinationError('owned RAM device metadata mismatch; refusal to format')
            run(['/usr/sbin/diskutil','eraseVolume','HFS+',self.label,self.device])
            info = plistlib.loads(run(['/usr/sbin/diskutil','info','-plist',self.device]))
            self.mount = pathlib.Path(info.get('MountPoint',''))
            if self.mount.parent != pathlib.Path('/Volumes') or self.mount.name != self.label or not os.path.ismount(self.mount):
                raise RamDestinationError('exact owned RAM mount unavailable')
            os.chmod(self.mount,0o700)
            self.folder = self.mount/'private-key'; self.folder.mkdir(mode=0o700)
            self.dir_fd = os.open(self.folder,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            if os.fstat(self.dir_fd).st_dev != self.mount.stat().st_dev: raise RamDestinationError('private directory escaped RAM filesystem')
            self.key_path = self.folder/'credential.p8'
            self.receipt = {'device':self.device,'backing':backing,'mount':str(self.mount),'mount_dev':self.mount.stat().st_dev,'private_directory_mode':'0700','key_mode':'0600','no_TMPDIR_mktemp_or_disk_fallback':True}
            self.verify()
            return self
        except BaseException:
            self.cleanup(); raise
    def verify(self):
        if not self.device or image_backing(self.device) != 'ram://'+str(self.sectors): raise RamDestinationError('RAM backing changed')
        info = plistlib.loads(run(['/usr/sbin/diskutil','info','-plist',self.device]))
        if pathlib.Path(info.get('MountPoint','')) != self.mount or not os.path.ismount(self.mount): raise RamDestinationError('RAM mount changed')
        if not self.dir_fd or os.fstat(self.dir_fd).st_dev != self.receipt['mount_dev']: raise RamDestinationError('RAM directory binding changed')
        current = self.folder.lstat(); bound = os.fstat(self.dir_fd)
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev,current.st_ino) != (bound.st_dev,bound.st_ino): raise RamDestinationError('consumer directory path changed')
        if self.key_identity is not None:
            current = self.key_path.lstat()
            if not stat.S_ISREG(current.st_mode) or (current.st_dev,current.st_ino) != self.key_identity: raise RamDestinationError('consumer key path changed')
    def write(self, data):
        self.verify()
        self.key_fd = os.open('credential.p8',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=self.dir_fd)
        try:
            if os.fstat(self.key_fd).st_dev != self.receipt['mount_dev']: raise RamDestinationError('key file escaped RAM filesystem')
            info=os.fstat(self.key_fd);self.key_identity=(info.st_dev,info.st_ino)
            view = memoryview(data)
            while len(view): view = view[os.write(self.key_fd,view):]
            os.fsync(self.key_fd)
        finally:
            os.close(self.key_fd); self.key_fd = None
        self.verify()
    def cleanup(self):
        errors=[]
        if self.key_fd is not None:
            try: os.close(self.key_fd)
            except OSError: errors.append('key_fd_close')
            self.key_fd=None
        if self.dir_fd is not None:
            try:
                fd=os.open('credential.p8',os.O_WRONLY|os.O_NOFOLLOW,dir_fd=self.dir_fd)
                try:
                    n=os.fstat(fd).st_size
                    if os.fstat(fd).st_dev != self.receipt.get('mount_dev'): raise RamDestinationError('cleanup filesystem mismatch')
                    while n: n-=os.write(fd,b'\0'*min(n,4096))
                finally: os.close(fd)
                os.unlink('credential.p8',dir_fd=self.dir_fd)
            except FileNotFoundError: pass
            except Exception: errors.append('key_cleanup')
            finally:
                try: os.close(self.dir_fd)
                except OSError: errors.append('directory_fd_close')
                self.dir_fd=None
        try:
            if self.folder and self.folder.exists(): self.folder.rmdir()
        except OSError: errors.append('private_directory_cleanup')
        detached=False
        if self.device:
            try:
                if image_backing(self.device) != 'ram://'+str(self.sectors): raise RamDestinationError('owned device changed; refuse detach')
                run(['/usr/bin/hdiutil','detach',self.device]);detached=True
            except Exception: errors.append('owned_RAM_detach')
        self.cleaned=detached
        if errors: raise RamDestinationError('RAM cleanup failed closed: '+','.join(errors))
    def __exit__(self,*_): self.cleanup()
