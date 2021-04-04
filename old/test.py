# coding: utf-8
import asyncio
import aiorun

async def ncount(queue_in):
    await asyncio.sleep(1)    
    while True:
        all_tasks = [f"{t.get_name()}" for t in asyncio.all_tasks()]
        print(",".join(all_tasks))
        all_queue = list(queue_in._queue)
        print(f"queue: {all_queue}")        
        if len(all_tasks) == 2: break        
        await asyncio.sleep(1)
    return
     
async def ntask(i, queue_in):
    # all_tasks = [f"{t.get_name()}" for t in asyncio.all_tasks()]
    # print(",".join(all_tasks))
    # print(str(len(all_tasks)))
    while True:
        t = await queue_in.get()
        print(f"[{i}] value {t}")
        if t == "KILL":
            break
        
        await asyncio.sleep(2+i)
    # all_tasks = [f"{t.get_name()}" for t in asyncio.all_tasks()]
    # print(",".join(all_tasks))
    # print(str(len(all_tasks)))
    
    print(f"{i} BYE")
    return

async def main():
    
    
    aqueue = asyncio.Queue()
    tasks = []
    for i in range(50):
        aqueue.put_nowait(i)
    for i in range(5):
        aqueue.put_nowait("KILL")
    
    for i in range(5):
        
        tasks.append(asyncio.create_task(ntask(i, aqueue), name=f"{i}"))
        
    tasks.append(asyncio.create_task(ncount(aqueue), name="count"))
    #tasks.append(asyncio.create_task(count()))
    await asyncio.wait(tasks)
    asyncio.get_running_loop().stop()
    return
    


aiorun.run(main())
    

