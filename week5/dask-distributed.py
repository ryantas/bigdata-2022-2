

import dask.array as da
x = da.random.random((4000,4000), chunks=(1000,1000))
y = da.exp(x).sum()

from dask.distributed import Client
client = Client("tcp://172.16.22.16:8786")
y.compute()
