"""本地数据读取层 —— 替代原代码的 hdfs.InsecureClient 读取链。

原模式:
    with hdfs_client.read(file_path) as reader:
        file_data = io.BytesIO(reader.read())
移植后:
    file_data = dataio.read_bytes(file_path)

原目录列举:
    files = [f for f in hdfs_client.list(hdfs_dir) if f.endswith('.nc4')]
移植后:
    files = dataio.list_files(manifest_name, suffix='.nc4')   # 返回完整路径
或直接列本地目录:
    files = dataio.list_dir(directory, suffix='.nc4')
"""
import io
import os

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# 数据文件句柄缓存（可选修复 N+1 读放大，见 config.FIX_READ_AMP）
_FIX_READ_AMP = os.environ.get('MGSI_FIX_READ_AMP', '0') == '1'
_h5py_handles = {}


def project_path(rel):
    """把项目相对路径转为绝对路径。"""
    if os.path.isabs(rel):
        return rel
    return os.path.join(_PROJECT_ROOT, rel)


def read_bytes(file_path):
    """读取本地文件为 BytesIO，接口语义与 hdfs_client.read 一致。"""
    with open(file_path, 'rb') as f:
        data = f.read()
    return io.BytesIO(data)


def open_h5py(file_path):
    """h5py 打开本地文件；FIX_READ_AMP=1 时缓存句柄避免重复整读。"""
    import h5py
    if _FIX_READ_AMP:
        if file_path not in _h5py_handles:
            _h5py_handles[file_path] = h5py.File(file_path, 'r')
        return _h5py_handles[file_path]
    return h5py.File(file_path, 'r')


def close_all():
    for f in list(_h5py_handles.values()):
        try:
            f.close()
        except Exception:
            pass
    _h5py_handles.clear()


def list_dir(directory, suffix=None):
    """列本地目录（替代 hdfs_client.list），返回完整路径。"""
    directory = project_path(directory) if not os.path.isabs(directory) else directory
    names = sorted(os.listdir(directory))
    if suffix is not None:
        names = [n for n in names if n.endswith(suffix)]
    return [os.path.join(directory, n) for n in names]


def read_manifest(manifest):
    """读清单文件：每行一个文件绝对路径（# 开头为注释）。"""
    path = project_path(manifest)
    files = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            files.append(line)
    return files


def list_files(manifest, suffix=None):
    """按清单名列数据文件（替代 hdfs_client.list(hdfs_dir)），返回完整路径。"""
    from mgsi_common import config
    manifest_path = config.MANIFESTS.get(manifest, manifest)
    files = read_manifest(manifest_path)
    if suffix is not None:
        files = [f for f in files if f.endswith(suffix)]
    return files
