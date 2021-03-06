#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
    for (int i=0;i<BLOCK_NUM;i++){
    memset(blocks[i],0,BLOCK_SIZE);
  }
}

void
disk::read_block(blockid_t id, char *buf)
{
  
  if(id < 0 || id > BLOCK_NUM || buf == NULL)
        return;
    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  
  if(id < 0 || id > BLOCK_NUM || buf == NULL)
        return;
  
  
    memcpy(blocks[id], buf, 512);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
    for (int i=IBLOCK(INODE_NUM,BLOCK_NUM)+1;i<BLOCK_NUM;i++){
    if (using_blocks[i]==0){
      using_blocks[i]=1;
      // printf("\tim:alloc block %d\n",i);
      return i;
    }
  }
  
 
  

    
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
   using_blocks[id] = 0;
    return;
 

}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = 512 * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
    for (int i=0;i<=BLOCK_NUM;i++){
    using_blocks[i] = 0;
  }

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}
#define INODE_MAP (IBLOCK(0,BLOCK_NUM)-2)

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */

  for (int i=1;i<INODE_NUM+1;i++){
    int map_block = INODE_MAP+(i-1)/BLOCK_SIZE;
    char* buf = new char[BLOCK_SIZE];
    bm->read_block(map_block,buf);
    if (buf[(i-1)%BLOCK_SIZE]==0){
      buf[(i-1)%BLOCK_SIZE] = 1;
      bm->write_block(map_block,buf);
      free(buf);
      // printf("\tim:alloc inode %d\t",i);
      inode_t* newnode = (inode_t*)malloc(sizeof(inode_t));
      memset(newnode,0,sizeof(inode_t));
      newnode->type = type;
      newnode->mtime = time(0);
      newnode->ctime = time(0);
      newnode->atime = time(0);
      printf("CREATETIME::%d\n",newnode->mtime);
      put_inode(i,newnode);
      free(newnode);
      return i;
    }

  }
  return 0;

   
  
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
   
    inode_t* res = get_inode(inum);
  if (res==NULL){
    printf("\tim: inode already freed %d",inum);
  }else{
    char* buf = new char[BLOCK_SIZE];
    memset(buf,0,BLOCK_SIZE);
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
    int map_block = INODE_MAP + (inum-1)/BLOCK_SIZE;
    free(buf);
    buf = new char[BLOCK_SIZE];
    bm->read_block(map_block,buf);
    buf[(inum-1)%BLOCK_SIZE] = 0;
    bm->write_block(map_block,buf);
    free(buf);
    printf("\tim:free inode %d\n",inum);
  }
  return;
  

  
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[512];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[512];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
 

  inode_t *ino = get_inode(inum);
  *size = ino->size;
  
  int chcnt = 0;
  int blcnt = 0;
  char* buf = new char[BLOCK_SIZE];
  bm->read_block(ino->blocks[blcnt],buf);

  (*buf_out) = new char[*size];
  for(int i=0;i<*size;i++){
    (*buf_out)[i] = buf[chcnt];
    chcnt++;
    if (chcnt==BLOCK_SIZE){
      chcnt = 0;
      blcnt++;
      if (blcnt<NDIRECT){
        bm->read_block(ino->blocks[blcnt],buf);
      }else{
        char* blist = new char[BLOCK_SIZE];
        bm->read_block(ino->blocks[NDIRECT],blist);
        blockid_t bid = ((blockid_t*)blist)[blcnt-NDIRECT];
        bm->read_block(bid,buf);
      }
    }
  }
  return;
  
  

  
  
}


/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf1, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
   int adjustsize;
   if (size>0)
    {adjustsize=((size-1)/512+1)*512;
    }
    else{
      adjustsize=512;
    }
   char *buf=new char[adjustsize];
   memcpy(buf,buf1,size);
   for (int i=size;i<adjustsize;i++)
   {
     buf[i]='\0';
   }
 
    inode_t* ino = get_inode(inum);
    ino->mtime=time(NULL);
    if (ino->type==extent_protocol::T_DIR)
    {
      ino->ctime=time(NULL);
    }
  //free old blocks
  int ori_size = ino->size;
  int blocknum = (ori_size-1)/BLOCK_SIZE+1;
  for (int i=0;i<blocknum;i++){
    if (i<NDIRECT){
      blockid_t bid = (ino->blocks)[i];
      bm->free_block(bid);
    }else{
      char* buf = new char[BLOCK_SIZE];
      bm->read_block((ino->blocks)[NDIRECT],buf);
      blockid_t bid = ((int*)buf)[i-NDIRECT];
      bm->free_block(bid);
    }
  }
  //write new file
  ino->size = size;
  blockid_t dirlist;
  blockid_t* blist = new blockid_t[BLOCK_SIZE/sizeof(blockid_t)];
  for (int b=0;b<(size-1)/BLOCK_SIZE+1;b++){
    blockid_t bid = bm->alloc_block();
    if (b<NDIRECT){
      bm->write_block(bid,buf+b*BLOCK_SIZE);
      ino->blocks[b] = bid;
    }
    if (b==NDIRECT){
      dirlist = bm->alloc_block();
      ino->blocks[b] = dirlist;
    }
    if (b>=NDIRECT){
      blist[b-NDIRECT] = bid;
      bm->write_block(bid,buf+b*BLOCK_SIZE);
    }
  }
  if (size>BLOCK_SIZE*NDIRECT){
    bm->write_block(dirlist,(char*)blist);
  }
  put_inode(inum,ino);
  free(ino);
  return;
  
  
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
   struct inode * ino=get_inode(inum);
   if(!ino)
        return;
   a.type=ino->type;
   a.atime=ino->atime;
   a.ctime=ino->ctime;
   a.mtime=ino->mtime;
   a.size=ino->size;
   delete ino;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
   struct inode* ino = get_inode(inum);
    if(!ino)
        return;
    uint32_t num = (ino->size + 511) / 512;
    for(uint32_t i=0; i<MIN(num, 100); i++)
        bm->free_block(ino->blocks[i]);
    blockid_t id = ino->blocks[100];
    if(num>100){
        char buf[512];
        bm->read_block(id, buf);
        uint32_t* ip = (uint32_t*)buf;
        for(uint32_t i=0; i<num-100; i++)
            bm->free_block(ip[i]);
        bm->free_block(id);
    }
    delete ino;
    free_inode(inum);
   
}
