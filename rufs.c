/*
 *  Copyright (C) 2023 CS416/518 Rutgers CS
 *	Tiny File System
 *	File:	rufs.c
 *
 */

#define FUSE_USE_VERSION 26
#define F 0
#define D 1

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>
#include <pthread.h>
#include "block.h"
#include "rufs.h"

#define RW 0
#define RWX 1

pthread_mutex_t mutex;

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here
int traverse_buffer(char *buff, int size, int start, int index, int start_block, struct inode* i_p, int flag);

//Stores the actual super block struct information for reference
struct superblock *s;

uint16_t blk_dirent_count = BLOCK_SIZE/sizeof(struct dirent);
uint16_t blk_inode_count = BLOCK_SIZE/sizeof(struct inode);


bitmap_t i_bitmap;
bitmap_t d_bitmap;

//void * tmp_buff;

void * fp;





 //TODO: Rename the global variables

// TODO: Change the name of all once whole coding is done
// This function will format the block, this block is filled with
// array of directory entries. Once the block is intialized with directory entries
// it is copied back to disk using bio_write.

		

/* 
 * Get available inode number from bitmap
 */
int get_avail_ino() {

	// Step 1: Read inode bitmap from disk
	void *tmp_buff = malloc(BLOCK_SIZE);
	//memset(tmp_buff, 0, BLOCK_SIZE);
	bio_read(s->i_bitmap_blk, (void *)tmp_buff);
	//i_bitmap = (bitmap_t)tmp_buff;
	memcpy((void *)i_bitmap, tmp_buff, MAX_INUM/8);
	
	for(int i = 0; i < MAX_INUM; i++)
	{
		//uint8_t bit = get_bitmap(i_bitmap, i);

		if (!get_bitmap(i_bitmap, i))
		{
			// Set the found bit in the data bitmap
			set_bitmap(i_bitmap, i);

			// Write it back to the disk
			memset(tmp_buff,0,BLOCK_SIZE);
			memcpy((void *)tmp_buff, (const void *)i_bitmap, BLOCK_SIZE);
			bio_write(s->i_bitmap_blk, (const void *) tmp_buff);
			
			// Return if found
			free(tmp_buff);
			return i;
		}
	}
	free(tmp_buff);
	// This will hit only if we did not find any free data block	
	return -1;
}


/* 
 * Get available data block number from bitmap
 */
int get_avail_blkno() {

	// Step 1: Read data block bitmap from disk
	void *tmp_buff = malloc(BLOCK_SIZE);
	//memset(tmp_buff, 0, BLOCK_SIZE);
	bio_read(s->d_bitmap_blk, (void *)tmp_buff);
	//d_bitmap = (bitmap_t)tmp_buff;
	memcpy((void *)d_bitmap, tmp_buff, MAX_INUM/8);
	
	// Step 2: Traverse data block bitmap to find an available slot

	// Step 3: Update data block bitmap and write to disk 

	for(int i = 0; i < MAX_DNUM; i++)
	{
		//uint8_t bit = get_bitmap(d_bitmap, i);

		if (!get_bitmap(d_bitmap, i))
		{
			// Set the found bit in the data bitmap
			set_bitmap(d_bitmap, i);

			// Write it back to the disk
			memset(tmp_buff,0,BLOCK_SIZE);
			memcpy((void *)tmp_buff, (const void *)d_bitmap, BLOCK_SIZE);
			bio_write(s->d_bitmap_blk, (const void *) tmp_buff);
			
			// Return if found
			free(tmp_buff);
			return i;
		}
	}
	free(tmp_buff);
	// This will hit only if we did not find any free data block	
	return -1;
}

/* 
 * inode operations
 */
int readi(uint16_t ino, struct inode *inode) {

// Step 1: Get the inode's on-disk block number

// Step 2: Get offset of the inode in the inode on-disk block

// Step 3: Read the block from disk and then copy into inode structure

	
	// Compute the block number where the passed inode exists on the disk
	uint16_t block_num = s->i_start_blk + (ino/blk_inode_count);

	// Compute the actual block number 
	//int block =  + block_num;

	// Compute the offset
	

	// Allocate block size to read the block from disk
	void * tmp_buff = malloc(BLOCK_SIZE);
	//memset(tmp_buff, 0, BLOCK_SIZE);

	// Do a read from disk
	bio_read(block_num, tmp_buff);

	struct inode * ptr;
	ptr = tmp_buff;
	
	uint16_t offset = (ino % blk_inode_count);
	// Move the pointer to the actual location where inode is present
	ptr = ptr + offset;

	// Copy it to the inode structure
	memcpy((void *)inode, (const void *) ptr, sizeof(struct inode));

	free(tmp_buff);
	return 0;
}

int writei(uint16_t ino, struct inode *inode) {
	

	// Step 1: Get the block number where this inode resides on disk
	uint16_t block_num = s->i_start_blk + (ino/blk_inode_count);


	void * tmp_buff = malloc(BLOCK_SIZE);

	bio_read(block_num, tmp_buff);

	struct inode * p;
	p = tmp_buff;

	// Step 2: Get the offset in the block where this inode resides on disk
	uint16_t offset = (ino % blk_inode_count);
	p += offset;

	memcpy((void *)p, (const void *) inode, sizeof(struct inode));

	// Step 3: Write inode to disk
	bio_write(block_num, (const void *) tmp_buff);

	free(tmp_buff);
	return 0;
}


/* 
 * directory operations
 */
int dir_find(uint16_t ino, const char *fname, size_t name_len, struct dirent *dirent) {

  

	
	struct inode curr_dir_ino;
	// Step 1: Call readi() to get the inode using ino (inode number of current directory)
	// Step 2: Get data block of current directory from inode
	readi(ino, &curr_dir_ino);

	
	// Step 3: Read directory's data block and check each directory entry.
	for(int i = 0; i < curr_dir_ino.size; i++)
	{
		int d_block_num = curr_dir_ino.direct_ptr[i];
		void * tmp_buff = malloc(BLOCK_SIZE);
	
		bio_read(d_block_num, tmp_buff);
		int link = 0;
		struct dirent * entry = tmp_buff; 

		

		for (int j = 0; j < blk_dirent_count; j++)
		{
			struct dirent *curr_entry = entry+j;

			if(curr_entry->valid)
			{
				char * name = curr_entry->name;
				
				int link_ind = 1;
				
				//If the name matches, then copy directory entry to dirent structure
				if (!strcmp(name, fname))
				{	
		
					link += 1;
					memcpy((void *) dirent, (void *)curr_entry, sizeof(struct dirent));
					free(tmp_buff);
					
					link_ind = link*curr_entry->valid;
					return 1;
				}
				
				link_ind = 0;
			}
		}

		free(tmp_buff);
	}
	return 0;
}

int dir_add(struct inode dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {

	
	// Step 1: Read dir_inode's data block and check each directory entry of dir_inode
	// Step 2: Check if fname (directory name) is already used in other entries
	for(int i = 0; i < dir_inode.size; i++)
	{

		uint32_t d_block_curr = dir_inode.direct_ptr[i];
		
	

		void * tmp_buff = malloc(BLOCK_SIZE);
		
		bio_read(d_block_curr, tmp_buff);
		
		struct dirent * entry = tmp_buff;

		for (int j = 0; j < blk_dirent_count; j++)
		{
			struct dirent *curr_entry = entry + j;

			if(curr_entry-> valid == 1)
			{
				char * curr_name = curr_entry-> name;

				if (!strcmp(curr_name, fname))
				{
					free(tmp_buff);
					return -1;
				}
			}
		}
		free(tmp_buff);
	}
	

	// Step 3: Add directory entry in dir_inode's data block and write to disk
	int exists = 0;
	for(int i = 0; i < dir_inode.size; i++)
	{
	
		int current = 0;
		
		int d_block_curr = dir_inode.direct_ptr[i];
		int done = 0;
		
		// Allocate the memory for buffer and read the corresponding data block from the disk
		void *tmp_buff = malloc(BLOCK_SIZE);
		//memset(tmp_buff, 0, BLOCK_SIZE);
		
		bio_read(d_block_curr, tmp_buff);

		struct dirent * entry = tmp_buff; 

		// Check if we are able to find any free directory entry to add
		for (int j = 0; j < blk_dirent_count; j++)
		{
			struct dirent *curr_entry = entry + j;
			if(!curr_entry->valid)
			{
			
				
				curr_entry->ino = f_ino*current;
				curr_entry->valid = 1;
				strcpy(curr_entry->name, fname);
				curr_entry->len = name_len;

				
				bio_write(d_block_curr, (const void *)tmp_buff);

				
				done = 1;
				break;
			}
			
		}
		if(done){
			break;
		}
		free(tmp_buff);
	}
	
	// Allocate a new data block for this directory if it does not exist
	if(!exists)
	{
		// Get the next available data block to add this directory
		int new_block = get_avail_blkno() + s->d_start_blk;


		
		struct dirent *temp = malloc(sizeof(struct dirent)*blk_dirent_count);
		
		memset(temp, 0, blk_dirent_count * sizeof(struct dirent));
		uint32_t data_block_base = 0;
		
		for (int i = 0; i < blk_dirent_count; i++)
		{
			temp[i].valid = 0;
		}
		
		void *tmp_buff= malloc(BLOCK_SIZE);
		memset(tmp_buff,0,BLOCK_SIZE);

		memcpy((void *)tmp_buff, (const void *)temp, sizeof(struct dirent)*blk_dirent_count);
		
		bio_write(new_block, (const void *)tmp_buff);
		data_block_base = s->d_start_blk;
		struct inode * new_dir_ino = malloc(sizeof(struct inode)); 

		readi(dir_inode.ino, new_dir_ino);
		new_dir_ino->size += 1;

		*((new_dir_ino->direct_ptr) + dir_inode.size) = new_block;

		writei(dir_inode.ino, new_dir_ino);

		memset(tmp_buff, 0, BLOCK_SIZE);


		bio_read(new_block, tmp_buff);
		free(temp);
		
		// Update directory 		inode													
		struct dirent *curr_entry = tmp_buff;
		curr_entry->valid = 1;
		curr_entry-> ino = f_ino;
		strcpy(curr_entry-> name, fname);
		curr_entry-> len = name_len;

			// Write directory entry
		bio_write(new_block, (const void *) tmp_buff);

		free(tmp_buff);

		free(new_dir_ino);
	}
	return 0;
}

//OPTIONAL
int dir_remove(struct inode dir_inode, const char *fname, size_t name_len) {

	// Step 1: Read dir_inode's data block and checks each directory entry of dir_inode
	
	// Step 2: Check if fname exist

	// Step 3: If exist, then remove it from dir_inode's data block and write to disk

	return 0;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, struct inode *inode) {
	
	
	// Step 1: Resolve the path name, walk through path, and finally, find its inode.
	// Implemented in using Recursion
	
	//Check if the string is empty
	if(path == NULL || *path =='\0'){
		return -1;
	}
	
	// Base case in a recursive call, which is the root directory
	if(strcmp(path, "/")==0)
	{
		readi(0, inode);
        	return 0;
	}

	
	
	// Get the base name and parent directory
	char * base_name = basename(strdup(path));
	char * dir_name = dirname(strdup(path));
	
	
	struct inode * temp_inode = (struct inode *)malloc(sizeof(struct inode));
	struct dirent * dir_entry = (struct dirent *)malloc(sizeof(struct dirent));


	if (get_node_by_path(dir_name, 0, temp_inode) == -1)
	{

		free(dir_entry);
		free(temp_inode);
		return -1;
	}
	
    	free(temp_inode);
	if (!dir_find(temp_inode->ino, base_name, strlen(base_name), dir_entry))
	{
	    free(dir_entry);
        	return -1;
    	}

    	readi(dir_entry->ino, inode);

    	free(dir_entry);

	return 0;
}

/* 
 * Make file system
 */
int rufs_mkfs() {


	// Call dev_init() to initialize (Create) Diskfile
	dev_init(diskfile_path);
	
	// write superblock information
	s->magic_num = MAGIC_NUM;
	s->max_inum = MAX_INUM;
	s->max_dnum = MAX_DNUM;
	
	s->i_bitmap_blk = 1;
	unsigned int tmp_blk_size = ((MAX_INUM/8)/BLOCK_SIZE > 1) ? ((MAX_INUM/8)+BLOCK_SIZE-1)/BLOCK_SIZE : 1;
	
	s->d_bitmap_blk = s->i_bitmap_blk + tmp_blk_size;
	
	tmp_blk_size = ((MAX_DNUM/8)/BLOCK_SIZE > 1) ? ((MAX_DNUM/8)+BLOCK_SIZE-1)/BLOCK_SIZE : 1;
	
	s->i_start_blk = s->d_bitmap_blk + tmp_blk_size;
	
	tmp_blk_size = ((MAX_INUM*sizeof(struct inode))/BLOCK_SIZE > 1) ? ((MAX_INUM*sizeof(struct inode))+BLOCK_SIZE-1)/BLOCK_SIZE : 1;
	
	s->d_start_blk = s->i_start_blk + tmp_blk_size;
	

	memcpy((void *)fp,  (const void *)s, sizeof(struct superblock));
	bio_write(0, (const void *) fp);
	

	// initialize inode bitmap
	memset(i_bitmap,0, MAX_INUM/8);

	// initialize data block bitmap
	memset(d_bitmap,0, MAX_DNUM/8);
	

	// update bitmap information for root directory
	set_bitmap(i_bitmap, 0);
	set_bitmap(d_bitmap, 0);
	
	void *tmp_buff = malloc(BLOCK_SIZE);
	memcpy((void *)tmp_buff, (const void *)i_bitmap, MAX_INUM/8);
	bio_write(s->i_bitmap_blk, (const void *) tmp_buff);
	
	memset(tmp_buff, 0, BLOCK_SIZE);
	memcpy((void *)tmp_buff, (const void *)i_bitmap, MAX_INUM/8);
	bio_write(s->d_bitmap_blk, (const void *) d_bitmap);


	// update inode for root directory

	struct inode r_inode;

	r_inode.link = 2;
	r_inode.ino = 0;
	r_inode.size = 1;
	r_inode.type = D;
	r_inode.valid = 1;
	r_inode.direct_ptr[0] = s->d_start_blk;
	r_inode.indirect_ptr[0] = s->d_start_blk;
	r_inode.vstat.st_mtime = time(NULL);
	r_inode.vstat.st_atime = time(NULL);
	r_inode.vstat.st_nlink = 2;
	r_inode.vstat.st_mode = S_IFDIR | 0755;
	
	memset(tmp_buff, 0, BLOCK_SIZE);
	memcpy((void *)tmp_buff, (const void *)&r_inode, sizeof(struct inode));
	bio_write(s->i_start_blk, (const void *)tmp_buff);
	
	//update the data block for the root directory
	struct dirent * tmp = (struct dirent *)malloc(blk_dirent_count * sizeof(struct dirent));
	memset(tmp, 0, blk_dirent_count * sizeof(struct dirent));

	for (int i = 0; i < blk_dirent_count; i++)
	{
		tmp[i].valid = 0;
	}

	memset(tmp_buff, 0, BLOCK_SIZE);
	memcpy((void *)tmp_buff, (const void *)tmp, sizeof(struct dirent)*blk_dirent_count);
	bio_write(s->d_start_blk, (const void *)tmp_buff);
	free(tmp);
	
	// Add the dirent of '.' for the root
	const char * filename = ".";
	dir_add(r_inode, 0, filename, 1);
	//printf("Reached RUFS MKFS end");
	
	
	free(tmp_buff);
	return 0;
}


/* 
 * FUSE file operations
 */
static void *rufs_init(struct fuse_conn_info *conn) {


  	pthread_mutex_lock(&mutex);
  	//printf("In RUFS INIT");
  	
	
	void *tmp_buff = malloc(BLOCK_SIZE);
	fp = malloc(BLOCK_SIZE);
	i_bitmap = (bitmap_t) malloc((MAX_INUM)/8);
	d_bitmap = (bitmap_t) malloc((MAX_DNUM)/8);
	
	// Step 1a: If disk file is not found, call mkfs
	
	// Step 1b: If disk file is found, just initialize in-memory data structures
	if(dev_open(diskfile_path) == 0){
		

  		// and read superblock from disk
		
		bio_read(0, (void *)fp);
		s = (struct superblock *)fp;
		
		bio_read(s->i_bitmap_blk,tmp_buff);

		memcpy((void *)i_bitmap, tmp_buff, MAX_INUM/8);

		memset(tmp_buff, 0, BLOCK_SIZE);

    		bio_read(s->d_bitmap_blk,tmp_buff);

    		memcpy((void *)d_bitmap, tmp_buff, MAX_DNUM/8);

	}
	else{
	
		rufs_mkfs();
	}
	
	pthread_mutex_unlock(&mutex);
	//printf("Reached RUFS INIT end");
	
	free(tmp_buff);
	return NULL;
}

static void rufs_destroy(void *userdata) {

	// Step 1: De-allocate in-memory data structures
	
	pthread_mutex_lock(&mutex);

	free(d_bitmap);
	
	free(i_bitmap);
	
	free(fp);

	// Step 2: Close diskfile
	dev_close();

	pthread_mutex_unlock(&mutex);
}

static int rufs_getattr(const char *path, struct stat *stbuf) {




	pthread_mutex_lock(&mutex);
	
	struct inode * inode_path = malloc(sizeof(struct inode));

	// Step 1: call get_node_by_path() to get inode from path
	if (get_node_by_path(path, 0, inode_path) == -1)
	{
		//Invaild path
		free(inode_path);
		pthread_mutex_unlock(&mutex);
		return -ENOENT;
	}

	// Step 2: fill attribute of file into stbuf from inode
	stbuf -> st_gid = getgid();
	time(&stbuf->st_mtime);
	stbuf->st_nlink  = 2;
	stbuf -> st_size = (inode_path-> size) * BLOCK_SIZE;
	stbuf->st_mode = (inode_path->type == F) ? S_IFREG | 0755: S_IFDIR | 0755; 
	stbuf -> st_uid = getuid();
	

	free(inode_path);

	pthread_mutex_unlock(&mutex);
	return 0;
}

static int rufs_opendir(const char *path, struct fuse_file_info *fi) {


	pthread_mutex_lock(&mutex);
	//printf("IN RUFS OPENDIR\n");

	struct inode * inode_path = malloc(sizeof(struct inode));

	// Step 1: Call get_node_by_path() to get inode from path
	
	if (get_node_by_path(path, 0, inode_path) == -1)
	{
		free(inode_path);
		pthread_mutex_unlock(&mutex);
		
		// Step 2: If not find, return -1
		return -1;
	}

	free(inode_path);
    	pthread_mutex_unlock(&mutex);
    	return 0;
}

static int rufs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {


	pthread_mutex_lock(&mutex);	

	struct inode * inode_path = malloc(sizeof(struct inode));
	
	// Step 1: Call get_node_by_path() to get inode from path
	int flag = 1;
	if (!(get_node_by_path(path, 0, inode_path)+flag))
	{
		free(inode_path);
		pthread_mutex_unlock(&mutex);
		return -1;
	}


	// all the data blocks belonging to the directory need to be checked
	for(int i = 0; i < inode_path->size; i++)
	{
		int d_block_curr = *((inode_path->direct_ptr) + i);

		void *tmp_buff = malloc(BLOCK_SIZE);
		
		bio_read(d_block_curr, tmp_buff);

		struct dirent * entry = tmp_buff;
		
		// Step 2: Read directory entries from its data blocks, and copy them to filler
		for (int j = 0; j < blk_dirent_count; j++)
		{
			struct dirent *curr_entry = entry + j;
			//If valid copy them to filler
			if(curr_entry->valid)
			{
				char * name = curr_entry-> name;
				// use filler to put all directory entries into the buffer
				filler(buffer, name, NULL, 0);
			}
		}
		free(tmp_buff);
	}
	free(inode_path);
	pthread_mutex_unlock(&mutex);
	return 0;
}


static int rufs_mkdir(const char *path, mode_t mode) {



	// Step 2: Call get_node_by_path() to get inode of parent directory



	pthread_mutex_lock(&mutex);

	// Step 1: Use dirname() and basename() to separate parent directory path and target directory name
	char * dir_name = dirname(strdup(path));
	char * base_name = basename(strdup(path));
	
	// Step 2: Call get_node_by_path() to get inode of parent directory

	struct inode * parent_inode = malloc(sizeof(struct inode));

	
	// Step 3: Call get_avail_ino() to get an available inode number
	if (get_node_by_path(dir_name, 0, parent_inode) == -1)
	{
		// Invalid Path
		pthread_mutex_unlock(&mutex);
		return 0;
	}
	
	// Step 3:
	int avail_inode = get_avail_ino();
	if(avail_inode == -1)
	{
		// No Space for file to add
		pthread_mutex_unlock(&mutex);
		return 0;
	}
	
	int prev_link = 0;

	// Step 4: Call dir_add() to add directory entry of target directory to parent directory

	if(dir_add(*parent_inode, avail_inode, base_name, strlen(base_name)) == -1)
	{
		// It is already present, exit!
		unset_bitmap(i_bitmap, avail_inode);
		
		void *tmp_buff = malloc(BLOCK_SIZE);
		memcpy((void *)tmp_buff, (const void *)i_bitmap, MAX_INUM/8);
		bio_write(s->i_bitmap_blk, (const void *) tmp_buff);
		
		pthread_mutex_unlock(&mutex);
		return -1;
	}
	
	

	// Step 5: Update inode for target directory
	readi(parent_inode->ino, parent_inode);
	parent_inode->link = parent_inode->link + 1 + prev_link;
	writei(parent_inode->ino, parent_inode);

	// Create a new inode for the base directory

	struct inode * inode_new = malloc(sizeof(struct inode));

	inode_new -> type = D;
	inode_new -> link = 2;
	inode_new -> ino = avail_inode;
	inode_new -> valid = 1;
	time(&(inode_new -> vstat).st_mtime);
	(inode_new->vstat).st_mode = RW;
	inode_new -> size = 0;

	// Creating 2 directory entries '.' and '..' for the base file and parent file
	dir_add(*inode_new, avail_inode, (const char *) ".", 1);
	readi(inode_new->ino, inode_new);
	dir_add(*inode_new, parent_inode->ino, (const char *)"..", 2);
	
	free(parent_inode);
	// Step 6: Call writei() to write inode to disk
	writei(avail_inode, inode_new);
	
	
	free(inode_new);
	
	pthread_mutex_unlock(&mutex);
	return 0;	
}

//OPTIONAL
static int rufs_rmdir(const char *path) {


	return 0;
}

static int rufs_releasedir(const char *path, struct fuse_file_info *fi) {

    return 0;
}

static int rufs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {


	pthread_mutex_lock(&mutex);
	//printf("IN Create\n");

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name
	
	char * dir_name = dirname(strdup(path));
	char * base_name = basename(strdup(path));


	struct inode * parent_inode = malloc(sizeof(struct inode));


	// Step 3: Call get_avail_ino() to get an available inode number
	int avail_inode = get_avail_ino();
	
	// Step 2: Call get_node_by_path() to get inode of parent directory
	if (get_node_by_path(dir_name, 0, parent_inode) == -1)
	{

		pthread_mutex_unlock(&mutex);
		return 0;
	}

	int flag = 1;
	if(!(avail_inode+flag))
	{
		// No Space for file to add
		pthread_mutex_unlock(&mutex);
		return 0;
	}


	// Step 4: Call dir_add() to add directory entry of target file to parent directory
	
	if(!(dir_add(*parent_inode, avail_inode, base_name, strlen(base_name))+flag))
	{
		pthread_mutex_unlock(&mutex);
		return -1;
	}
	free(parent_inode);

	// Step 5: Update inode for target file
	struct inode * inode_new = malloc(sizeof(struct inode));

	inode_new -> ino = avail_inode;
	inode_new -> valid = 1;
	inode_new -> link = 1;
	inode_new -> size = 0;
	time(&(inode_new -> vstat).st_mtime);
	(inode_new -> vstat).st_mode = mode;
	inode_new -> type = F;
	
	
	// Step 6: Call writei() to write inode to disk
	writei(avail_inode, inode_new);

	
	free(inode_new);
	
	
	pthread_mutex_unlock(&mutex);
	return 0;
}

	

static int rufs_open(const char *path, struct fuse_file_info *fi) {

	pthread_mutex_lock(&mutex);
	//printf("IN RUFS OPEN\n");

	struct inode * parent_inode = malloc(sizeof(struct inode));

	// Step 1: Call get_node_by_path() to get inode from path
	if (get_node_by_path(path, 0, parent_inode) == -1)
	{
		free(parent_inode);
		pthread_mutex_unlock(&mutex);
		
		// Step 2: If not find, return -1
		return -1;
	}
	
	free(parent_inode);
	pthread_mutex_unlock(&mutex);
	return 0;
}

static int rufs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	
	pthread_mutex_lock(&mutex);

	
	// Step 1: You could call get_node_by_path() to get inode from path
	struct inode * inode_path = malloc(sizeof(struct inode));


    	if (get_node_by_path(path, 0, inode_path) == -1)
	{
            free(inode_path);
	    pthread_mutex_unlock(&mutex);
	    
            return 0;
    	}

	int file_size = (inode_path->size) * BLOCK_SIZE;
	
	//check if the reading bounds are valid
	if((((inode_path->size) * BLOCK_SIZE) < size) || (((inode_path->size) * BLOCK_SIZE) < offset))
	{
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	// Step 2: Based on size and offset, read its data blocks from disk
	
	int start = offset % BLOCK_SIZE;
	int index = offset/BLOCK_SIZE;
	int start_block = *((inode_path->direct_ptr) + index);
	int buff_size = size;

	if ((((inode_path->size) * BLOCK_SIZE) - offset < size))
	{
		pthread_mutex_unlock(&mutex);
		return 0;
	}

	char * buff = (char *)buffer;
	
	// Step 3: copy the correct amount of data from offset to buffer
	
	int fin = traverse_buffer(buff, size, start, index, start_block, inode_path, 0);

	free(inode_path);
	pthread_mutex_unlock(&mutex);
	
	// return the amount of bytes you copied to buffer
	return buff_size;
}

int traverse_buffer(char *buff, int size, int start, int index, int start_block, struct inode* i_p, int flag){
	while(size > 0){
		int remain = BLOCK_SIZE - start;

		void * tmp_buff = malloc(BLOCK_SIZE);

		bio_read(start_block, tmp_buff);

		char * buffTemp = (char *)tmp_buff;

		char * addr = buffTemp + start;
		
		if (size <= remain)
		{
			//for read
			if(flag == 0){
				memcpy((void *)buff, (void *)addr, size);
			}
			
			//for write
			else if(flag == 1){
				memcpy((void *)addr, (void *)buff, size);
				bio_write(start_block,tmp_buff);
			}
			free(tmp_buff);
			break;
		}

		if(flag == 0){
				memcpy((void *)buff, (void *)addr, remain);
			}
			
			//for write
		else if(flag == 1){
				memcpy((void *)addr, (void *)buff, remain);
				bio_write(start_block,tmp_buff);
			}
			
		buff += remain;
		size -= remain;
		start = 0;
		index ++;
		
		if(flag == 1){
			if(i_p->size < (index+1)){
				time(&(i_p -> vstat).st_mtime);
				writei(i_p -> ino, i_p);
		
				pthread_mutex_unlock(&mutex);
				return 1;
			}
		}
		
		start_block = *((i_p -> direct_ptr) + index); 
		free(tmp_buff);
			
		}
	
	return 0;
	
}

static int rufs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {


	// Step 2: Based on size and offset, read its data blocks from disk



	// Note: this function should return the amount of bytes you write to disk

	pthread_mutex_lock(&mutex);

	int buff_size = size;
	struct inode * inode_path = malloc(sizeof(struct inode));

   
	// Step 1: You could call get_node_by_path() to get inode from path
    	if (get_node_by_path(path, 0, inode_path) == -1)
    	{
        	free(inode_path);
		pthread_mutex_unlock(&mutex);
        	return 0;
    	}

	// Step 2: Based on size and offset, read its data blocks from disk
	
	int index = offset/BLOCK_SIZE;
    	int start = offset % BLOCK_SIZE;


	int existing_blocks = inode_path->size;
	int file_size = ((offset/BLOCK_SIZE) * BLOCK_SIZE) + (offset % BLOCK_SIZE) + size;

	// Number of blocks needed
	int total_blocks = (file_size+BLOCK_SIZE-1)/BLOCK_SIZE;
	
	int dev = total_blocks - existing_blocks;

	if (dev > 0)
	{
		//we need to add extra blocks
		for (int j = 0; j < dev; j ++)
		{
			int avail_blk = get_avail_blkno();
			if (avail_blk == -1)
			{
				break;
			}
			*((inode_path->direct_ptr) + existing_blocks + j) = avail_blk + s->d_start_blk;
			inode_path->size += 1;
		}
	}

	if (!inode_path->size)
	{
		pthread_mutex_unlock(&mutex);
		return 0;
	}
	if (inode_path->size < (index + 1))
	{
		pthread_mutex_unlock(&mutex);
		return 0 ;
	}


	int starting_blk = *((inode_path->direct_ptr) + index);
	char * buff = (char *)buffer;
	// Step 3: Write the correct amount of data from offset to disk
    int fin = traverse_buffer(buff, size, start, index, starting_blk, inode_path, 1);
    if(fin==1){
    	return (buff_size - size);
    }

	time(&(inode_path -> vstat).st_mtime);

	writei(inode_path -> ino, inode_path);
	
	free(inode_path);
	pthread_mutex_unlock(&mutex);
	return buff_size;
}

// OPTIONAL
static int rufs_unlink(const char *path) {

	// Step 1: Use dirname() and basename() to separate parent directory path and target file name

	// Step 2: Call get_node_by_path() to get inode of target file

	// Step 3: Clear data block bitmap of target file

	// Step 4: Clear inode bitmap and its data block

	// Step 5: Call get_node_by_path() to get inode of parent directory

	// Step 6: Call dir_remove() to remove directory entry of target file in its parent directory

	return 0;
}

static int rufs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int rufs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int rufs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}





static struct fuse_operations rufs_ope = {
	.init		= rufs_init,
	.destroy	= rufs_destroy,

	.getattr	= rufs_getattr,
	.readdir	= rufs_readdir,
	.opendir	= rufs_opendir,
	.releasedir	= rufs_releasedir,
	.mkdir		= rufs_mkdir,
	.rmdir		= rufs_rmdir,

	.create	= rufs_create,
	.open		= rufs_open,
	.read 		= rufs_read,
	.write		= rufs_write,
	.unlink		= rufs_unlink,

	.truncate   = rufs_truncate,
	.flush      = rufs_flush,
	.utimens    = rufs_utimens,
	.release	= rufs_release
};


int main(int argc, char *argv[]) {
	int fuse_stat;

	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");

	fuse_stat = fuse_main(argc, argv, &rufs_ope, NULL);

	return fuse_stat;
}

