#include "testfs.h"
#include "list.h"
#include "super.h"
#include "block.h"
#include "inode.h"

#define MAX_NUM_OF_LOGICAL_BLOCKS 4196362
#define MAX_FILE_SYSTEM_BLOCKS 1048576
#define MAX_NUM_OF_INODES 16384
#define MAX_NR_DOUBLY_INDIRECT_BLOCKS 4194304

/* given logical block number, read the corresponding physical block into block.
 * return physical block number.
 * returns 0 if physical block does not exist.
 * returns negative value on other errors. */
static int
testfs_read_block(struct inode *in, int log_block_nr, char *block) {
    int phy_block_nr = 0;

    assert(log_block_nr >= 0);



    if (log_block_nr < NR_DIRECT_BLOCKS) {
        phy_block_nr = (int) in->in.i_block_nr[log_block_nr];
    } else {
        log_block_nr -= NR_DIRECT_BLOCKS;

        if (log_block_nr < NR_INDIRECT_BLOCKS) {
            // should return singly indirect block

            if (in->in.i_indirect > 0) {
                //indirect block is already allocated
                read_blocks(in->sb, block, in->in.i_indirect, 1);
                phy_block_nr = ((int *) block)[log_block_nr];
            } else {
                //indirect block is not allocated 
                return phy_block_nr;
            }
        } else {

            log_block_nr -= NR_INDIRECT_BLOCKS;

            if (log_block_nr < MAX_NR_DOUBLY_INDIRECT_BLOCKS) {

                if (in->in.i_dindirect > 0) {
                    //doubly indirect block is already allocated
                    read_blocks(in->sb, block, in->in.i_dindirect, 1);

                    if (((int*) block)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)] > 0) {
                        //indirect block from the doubly indirect block is allocated
                        read_blocks(in->sb, block, ((int*) block)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)], 1);
                        //access the direct block number linked to doubly indirect block
                        phy_block_nr = ((int*) block)[(int) (log_block_nr % NR_INDIRECT_BLOCKS)];
                    } else {
                        //indirect block is not allocated
                        return phy_block_nr;
                    }
                } else {
                    //doubly_indirect block is not allocated
                    return phy_block_nr;
                }
            } else {
                //block nr is greater than the max logical block numer
                return -EFBIG;
            }
        }
    }

    if (phy_block_nr > 0) {
        read_blocks(in->sb, block, phy_block_nr, 1);
    } else {
        /* we support sparse files by zeroing out a block that is not
         * allocated on disk. */
        bzero(block, BLOCK_SIZE);
    }
    return phy_block_nr;
}

int
testfs_read_data(struct inode *in, char *buf, off_t start, size_t size) {
    char block[BLOCK_SIZE];
    long block_nr = start / BLOCK_SIZE; // logical block number in the file
    long block_ix = start % BLOCK_SIZE; //  index or offset in the block
    int ret;
    size_t byte_read = 0;

    assert(buf);
    if (start + (off_t) size > in->in.i_size) {
        size = in->in.i_size - start;
    }

    bzero(block, BLOCK_SIZE);

    if (block_ix + size > BLOCK_SIZE) {

        testfs_read_block(in, block_nr, block);
        memcpy(buf, block + block_ix, BLOCK_SIZE - block_ix);
        byte_read = BLOCK_SIZE - block_ix;
        block_nr++;

        while (byte_read != size) {

            if ((ret = testfs_read_block(in, block_nr, block)) < 0)
                return ret;

            if (size - byte_read >= BLOCK_SIZE) {
                memcpy(buf + byte_read, block, BLOCK_SIZE);
                byte_read = byte_read + BLOCK_SIZE;
                block_nr++;
            } else {
                memcpy(buf + byte_read, block, size - byte_read);
                byte_read = size;
            }

        }
        return size;
    }

    if ((ret = testfs_read_block(in, block_nr, block)) < 0)
        return ret;
    memcpy(buf, block + block_ix, size);
    /* return the number of bytes read or any error */
    return size;
}

/* given logical block number, allocate a new physical block, if it does not
 * exist already, and return the physical block number that is allocated.
 * returns negative value on error. */
static int
testfs_allocate_block(struct inode *in, int log_block_nr, char *block) {
    int phy_block_nr;
    char indirect[BLOCK_SIZE];
    char d_indirect[BLOCK_SIZE];
    int indirect_allocated = 0;
    int doubly_indirect_allocated = 0;
    int indirect_in_dindirect_allocated = 0;


    assert(log_block_nr >= 0);


    bzero(d_indirect, BLOCK_SIZE);
    bzero(indirect, BLOCK_SIZE);

    phy_block_nr = testfs_read_block(in, log_block_nr, block);

    /* phy_block_nr > 0: block exists, so we don't need to allocate it, 
       phy_block_nr < 0: some error */
    if (phy_block_nr != 0)
        return phy_block_nr;

    /* allocate a direct block */
    if (log_block_nr < NR_DIRECT_BLOCKS) {
        //it should be empty ?
        assert(in->in.i_block_nr[log_block_nr] == 0);
        phy_block_nr = testfs_alloc_block_for_inode(in);
        if (phy_block_nr >= 0) {
            in->in.i_block_nr[log_block_nr] = phy_block_nr;
        }
        return phy_block_nr;
    }

    log_block_nr -= NR_DIRECT_BLOCKS;
    if (log_block_nr < NR_INDIRECT_BLOCKS) {

        if (in->in.i_indirect == 0) { /* allocate an indirect block */
            bzero(indirect, BLOCK_SIZE);
            phy_block_nr = testfs_alloc_block_for_inode(in);
            if (phy_block_nr < 0) {
                return phy_block_nr;
            }

            indirect_allocated = 1;
            in->in.i_indirect = phy_block_nr;
        } else { /* read indirect block */
            read_blocks(in->sb, indirect, in->in.i_indirect, 1);
        }

        /* allocate direct block */
        assert(((int *) indirect)[log_block_nr] == 0);
        phy_block_nr = testfs_alloc_block_for_inode(in);

        if (phy_block_nr >= 0) {
            /* update indirect block */
            ((int *) indirect)[log_block_nr] = phy_block_nr;
            write_blocks(in->sb, indirect, in->in.i_indirect, 1);
        } else if (indirect_allocated) {
            /* there was an error while allocating the direct block, 
             * free the indirect block that was previously allocated */


            testfs_free_block_from_inode(in, in->in.i_indirect);
            in->in.i_indirect = 0;
            indirect_allocated = 0;
        }
        return phy_block_nr;
    }

    log_block_nr -= NR_INDIRECT_BLOCKS;

    if (log_block_nr < MAX_NR_DOUBLY_INDIRECT_BLOCKS) {
        //doubly indirect block should be implemented

        if (in->in.i_dindirect == 0) {
            //allocate a doubly indirect block

            phy_block_nr = testfs_alloc_block_for_inode(in);

            if (phy_block_nr < 0) {
                return phy_block_nr;
            }

            doubly_indirect_allocated = 1;
            in->in.i_dindirect = phy_block_nr;

        } else {
            //its already allocated so just read
            read_blocks(in->sb, d_indirect, in->in.i_dindirect, 1);
        }

        if (((int*) d_indirect)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)] == 0) {
            //the linked indirect block is not allocated

            phy_block_nr = testfs_alloc_block_for_inode(in);

            if (phy_block_nr < 0) {
                if (doubly_indirect_allocated) {
                    testfs_free_block_from_inode(in, in->in.i_dindirect);
                    in->in.i_dindirect = 0;
                    doubly_indirect_allocated = 0;
                }

                return phy_block_nr;
            }
            indirect_in_dindirect_allocated = 1;
            ((int*) d_indirect)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)] = phy_block_nr;
            write_blocks(in->sb, d_indirect, in->in.i_dindirect, 1);
        } else {
            //the linked indirect block already exists
            read_blocks(in->sb, indirect, ((int*) d_indirect)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)], 1);
        }

        //now everything until single indirect block is allocated or read
        //allocated direct block
        assert(((int*) indirect)[(int) (log_block_nr % NR_INDIRECT_BLOCKS)] == 0);
        //direct block doesnt exist
        //this condition is always true
        phy_block_nr = testfs_alloc_block_for_inode(in);

        if (phy_block_nr < 0) {
            if (indirect_in_dindirect_allocated > 0) {
                /* there was an error while allocating the direct block, 
                 * free the indirect block that was previously allocated */
                testfs_free_block_from_inode(in, ((int*) d_indirect)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)]);
            }
            if (doubly_indirect_allocated) {
                testfs_free_block_from_inode(in, in->in.i_dindirect);
                in->in.i_dindirect = 0;
                doubly_indirect_allocated = 0;
            } else {
                write_blocks(in->sb, d_indirect, in->in.i_dindirect, 1);
            }
            //                testfs_free_block_from_inode(in, phy_block_nr);   
        } else {
            /* update indirect block */
            ((int *) indirect)[(int) (log_block_nr % NR_INDIRECT_BLOCKS)] = phy_block_nr;
            write_blocks(in->sb, indirect, ((int*) d_indirect)[(int) (log_block_nr / NR_INDIRECT_BLOCKS)], 1);
        }
        return phy_block_nr;


    } else {
        return -EFBIG;
    }


}

int
testfs_write_data(struct inode *in, const char *buf, off_t start, size_t size) {
    char block[BLOCK_SIZE];
    long block_nr = start / BLOCK_SIZE; // logical block number in the file
    long block_ix = start % BLOCK_SIZE; //  index or offset in the block
    int ret;
   
    size_t byte_written = 0;


    bzero(block, BLOCK_SIZE);


    if (block_ix + size > BLOCK_SIZE) {

        ret = testfs_allocate_block(in, block_nr, block);
        if (ret < 0) {
            return ret;

        }
        memcpy(block + block_ix, buf, BLOCK_SIZE - block_ix);
        write_blocks(in->sb, block, ret, 1);
        byte_written = BLOCK_SIZE - block_ix;
        block_nr++;

        while (byte_written != size) {


            ret = testfs_allocate_block(in, block_nr, block);
            if (ret < 0) {
                if (size > 0)
                    in->in.i_size = MAX(in->in.i_size, start + (off_t) byte_written);
                in->i_flags |= I_FLAGS_DIRTY;
                return ret;
            }

            if (size - byte_written >= BLOCK_SIZE) {
                memcpy(block, buf + byte_written, BLOCK_SIZE);
                write_blocks(in->sb, block, ret, 1);
                byte_written = byte_written + BLOCK_SIZE;
                block_nr++;
            } else {
                memcpy(block, buf + byte_written, size - byte_written);
                write_blocks(in->sb, block, ret, 1);
                byte_written = size;
            }

        }

        /* increment i_size by the number of bytes written. */
        if (size > 0)
            in->in.i_size = MAX(in->in.i_size, start + (off_t) size);
        in->i_flags |= I_FLAGS_DIRTY;
        return size;
    }

    /* ret is the newly allocated physical block number */
    ret = testfs_allocate_block(in, block_nr, block);
    if (ret < 0) {
        if (size > 0)
            in->in.i_size = MAX(in->in.i_size, start + (off_t) size);
        in->i_flags |= I_FLAGS_DIRTY;
        return ret;
    }
    memcpy(block + block_ix, buf, size);
    write_blocks(in->sb, block, ret, 1);
    /* increment i_size by the number of bytes written. */
    if (size > 0)
        in->in.i_size = MAX(in->in.i_size, start + (off_t) size);
    in->i_flags |= I_FLAGS_DIRTY;
    /* return the number of bytes written or any error */
    return size;
}

int
testfs_free_blocks(struct inode *in) {
    int i;
    int e_block_nr;

    /* last logical block number */
    e_block_nr = DIVROUNDUP(in->in.i_size, BLOCK_SIZE);

    /* remove direct blocks */
    for (i = 0; i < e_block_nr && i < NR_DIRECT_BLOCKS; i++) {
        if (in->in.i_block_nr[i] == 0)
            continue;
        testfs_free_block_from_inode(in, in->in.i_block_nr[i]);
        in->in.i_block_nr[i] = 0;
    }
    e_block_nr -= NR_DIRECT_BLOCKS;

    /* remove indirect blocks */
    if (in->in.i_indirect > 0) {
        char block[BLOCK_SIZE];
        assert(e_block_nr > 0);
        read_blocks(in->sb, block, in->in.i_indirect, 1);
        for (i = 0; i < e_block_nr && i < NR_INDIRECT_BLOCKS; i++) {
            if (((int *) block)[i] == 0)
                continue;
            testfs_free_block_from_inode(in, ((int *) block)[i]);
            ((int *) block)[i] = 0;
        }
        testfs_free_block_from_inode(in, in->in.i_indirect);
        in->in.i_indirect = 0;
    }

    e_block_nr -= NR_INDIRECT_BLOCKS;
    /* handle double indirect blocks */
    if (e_block_nr > 0) {
        int deleted = 0;
        char d_indirect_block[BLOCK_SIZE];
        assert(e_block_nr > 0);
        read_blocks(in->sb, d_indirect_block, in->in.i_dindirect, 1);

        for (i = 0; deleted < e_block_nr && i < NR_INDIRECT_BLOCKS; i++) {
            if (((int*) d_indirect_block)[i] > 0) {
                //indirect block is allocated
                char indirect_block[BLOCK_SIZE];
                read_blocks(in->sb, indirect_block, ((int*) d_indirect_block)[i], 1);
                for (int j = 0; deleted < e_block_nr && j < NR_INDIRECT_BLOCKS; j++) {
                    //direct block inside indirect block is allocated
                    testfs_free_block_from_inode(in, ((int *) indirect_block)[j]);
                    ((int *) indirect_block)[j] = 0;
                    deleted++;
                }
                testfs_free_block_from_inode(in, ((int *) d_indirect_block)[i]);
                ((int *) d_indirect_block)[i] = 0;
            } else {
                deleted = deleted + NR_INDIRECT_BLOCKS;
            }
        }
        testfs_free_block_from_inode(in, in->in.i_dindirect);
        in->in.i_dindirect = 0;
    }

    in->in.i_size = 0;
    in->i_flags |= I_FLAGS_DIRTY;
    return 0;
}
