/*
 *  chidb - a didactic relational database management system
 *
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb
 * file" and "file of B-Trees" are essentially equivalent terms).
 *
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
 *
 */

/*
 *  Copyright (c) 2009-2015, The University of Chicago
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or withsend
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  - Neither the name of The University of Chicago nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software withsend specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY send OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <chidb/log.h>
#include "chidbInt.h"
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"


/* Open a B-Tree file
 *
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 *
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *       created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *       newly created BTree.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    Pager* pager;
    // the header, and static arrays to make checking header easier
    uint8_t header[100];
    uint8_t fourZeroes[] = {0x00, 0x00, 0x00, 0x00};
    uint8_t zeroAndOne[] = {0x00, 0x00, 0x00, 0x01};
    uint8_t pageCacheSize[] = {0x00, 0x00, 0x4E, 0x20}; 
    // Stores errors from function calls
    int err; 
    FILE* file = fopen(filename, "r");
    bool newFile = false;
    
    if(file == NULL) {
        // File does not exist yet
        fprintf(stderr, "Opening file!\n");
        newFile = true;
    } else {
        fseek (file, 0, SEEK_END);
        int size = ftell(file);
        if(size == 0) {
            // File exists, but is empty
            newFile = true;

        }
        fclose(file);
    }

    // Initialize pager 
    if((err = chidb_Pager_open(&pager, filename)) != CHIDB_OK) {
        return err;
    }
    // Create a BTree and set members of BTree and Database
    *bt = (BTree*) malloc(sizeof(BTree));
    if(*bt == NULL) {
        return CHIDB_ENOMEM;
    }
    (*bt)->pager = pager;
    (*bt)->db = db;
    db->bt = *bt;

    if(!newFile) {
        if (chidb_Pager_readHeader(pager, header) != CHIDB_OK) {
            return CHIDB_ECORRUPTHEADER;
        }
        // Check header is correct, just hardcoded checking, nothing special    
        if( !memcmp("SQLite format 3", header, 0x0F) &&
            !memcmp((uint8_t[]){ 0x01, 0x01, 0x00, 0x40, 0x20, 0x20 }, 
                &header[0x12], 6) && 
            !memcmp(fourZeroes, &header[0x20], 4) &&
            !memcmp(fourZeroes, &header[0x24], 4) &&
            !memcmp(zeroAndOne, &header[0x2C], 4) &&
            !memcmp(fourZeroes, &header[0x34], 4) &&
            !memcmp(zeroAndOne, &header[0x38], 4) &&
            !memcmp(fourZeroes, &header[0x40], 4) &&
            !memcmp(fourZeroes, &header[HEADER_FILECHANGE], 4) &&
            !memcmp(fourZeroes, &header[HEADER_SCHEMA], 4) &&
            !memcmp(pageCacheSize, &header[HEADER_PAGECACHESIZE], 4) &&
            !memcmp(fourZeroes, &header[HEADER_COOKIE], 4)
        ) {
            // If we made it here, the header is correct, set page size
            uint16_t pageSize = get2byte(&header[HEADER_PAGESIZE]);
            chidb_Pager_setPageSize(pager, pageSize);
        } else {
            return CHIDB_ECORRUPTHEADER;
        }

    } else {
        chidb_Pager_setPageSize(pager, DEFAULT_PAGE_SIZE);
        pager->n_pages = 0;
        npage_t npage;
        return chidb_Btree_newNode((*bt), &npage, PGTYPE_TABLE_LEAF);   
    }

    return CHIDB_OK;
}


/* Close a B-Tree file
 *
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 *
 * Parameters
 * - bt: B-Tree file to close
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{

    chidb_Pager_close(bt->pager);
    free(bt);
    return CHIDB_OK;
}


/* Loads a B-Tree node from disk
 *
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly creater BTreeNode
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    MemPage* page;
    int err;

     if ( !(*btn = (BTreeNode*) malloc(sizeof(BTreeNode)) ) ) {
        return CHIDB_ENOMEM;
    }

    // load page
    if((err = chidb_Pager_readPage(bt->pager, npage, &page)) != CHIDB_OK) {
        return err;
    }
    
    uint8_t* data = page->data;

    if(npage == 1) {
        // first page has header, so offset by 100 bytes
        data += 100;
    }

    (*btn)->page = page;
    (*btn)->type = *data;
    (*btn)->free_offset = get2byte(data+1);
    (*btn)->n_cells = get2byte(data+3);
    (*btn)->cells_offset = get2byte(data+5);
    if((*btn)->type == 0x05 || (*btn)->type == 0x02) {
        // Only internal nodes have right page, and offset starts at 12
        (*btn)->right_page = get4byte(data+8);
        (*btn)->celloffset_array = data+12;
    } else {
        (*btn)->celloffset_array = data+8;
    }
    
    return CHIDB_OK;
}


/* Frees the memory allocated to an in-memory B-Tree node
 *
 * Frees the memory allocated to an in-memory B-Tree node, and
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
    chidb_Pager_releaseMemPage(bt->pager, btn->page);   
    free(btn);

    return CHIDB_OK;
}


/* Create a new B-Tree node
 *
 * Allocates a new page in the file and initializes it as a B-Tree node.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *          was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    // load page
    
    chidb_Pager_allocatePage(bt->pager, npage);
    return chidb_Btree_initEmptyNode(bt, *npage, type);
}

/* Initialize a B-Tree node
 *
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
    int err;
    MemPage* page;
    if((err = chidb_Pager_readPage(bt->pager, npage, &page) != CHIDB_OK)) {
        return err;
    }
        

    uint8_t* data = page->data;

    if(npage == 1) {
        // Write header on first page
        sprintf((char*) data, "SQLite format 3");
        data = page->data + HEADER_PAGESIZE;
        put2byte(data, bt->pager->page_size);
        data = page->data + HEADER_JUNK;
        *(data++) = 0x01;
        *(data++) = 0x01;
        *(data++) = 0x00;
        *(data++) = 0x40;
        *(data++) = 0x20;
        *(data++) = 0x20;
        
        data = page->data + HEADER_FILECHANGE;
        put4byte(data, 0);

        data = page->data + HEADER_EMPTY;
        put4byte(data, 0);
        put4byte(data+4, 0);

        data = page->data + HEADER_SCHEMA;
        put4byte(data, 0);

        data = page->data + HEADER_ONE;
        put4byte(data, 1);
        
        data = page->data + HEADER_PAGECACHESIZE;
        put4byte(data, 20000);

        data = page->data + HEADER_EMPTYONE;
        put4byte(data, 0);
        put4byte(data+4, 1);

        data = page->data + HEADER_COOKIE;
        put4byte(data, 0);

        data = page->data + HEADER_ZERO;
        put4byte(data, 0);

        data = page->data + HEADER_END + 1;

        // Add free offset, which is different if not first cell
        // Free offset also depends on if the node is internal
        if(type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL) {
            put2byte(data + PGHEADER_FREE_OFFSET, HEADER_END + 1 
                    + INTPG_CELLSOFFSET_OFFSET);
        } else {
            put2byte(data + PGHEADER_FREE_OFFSET, HEADER_END + 1
                    + LEAFPG_CELLSOFFSET_OFFSET);
        }
    } else {
        if(type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL) {
            put2byte(data + PGHEADER_FREE_OFFSET, INTPG_CELLSOFFSET_OFFSET);
        } else {
            put2byte(data + PGHEADER_FREE_OFFSET, LEAFPG_CELLSOFFSET_OFFSET);
        }
    }

    *(data + PGHEADER_PGTYPE_OFFSET) = type;

    put2byte(data + PGHEADER_NCELLS_OFFSET, 0);
    put2byte(data + PGHEADER_CELL_OFFSET, bt->pager->page_size);
    *(data + PGHEADER_ZERO_OFFSET) = 0;
    if(type == INTPG_CELLSOFFSET_OFFSET || type == LEAFPG_CELLSOFFSET_OFFSET) {
        put4byte(data + PGHEADER_RIGHTPG_OFFSET, 0);
    }

    if((err = chidb_Pager_writePage(bt->pager, page)) != CHIDB_OK) {
        return err;
    }

    return chidb_Pager_releaseMemPage(bt->pager, page);
}



/* Write an in-memory B-Tree node to disk
 *
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
    MemPage* page = btn->page;

    uint8_t* data = page->data;
    if(page->npage == 1) {
        data += 100;
    }

    *(data + PGHEADER_PGTYPE_OFFSET) = btn->type;
    put2byte(data + PGHEADER_FREE_OFFSET, btn->free_offset);
    put2byte(data + PGHEADER_NCELLS_OFFSET, btn->n_cells);
    put2byte(data + PGHEADER_CELL_OFFSET, btn->cells_offset);
    
    if(btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL) {
        put4byte(data + PGHEADER_RIGHTPG_OFFSET, btn->right_page);
    }

    return chidb_Pager_writePage(bt->pager, page);
}


/* Read the contents of a cell
 *
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *  1. Find out the offset of the requested cell.
 *  2. Read the cell from the in-memory page, and parse its
 *     contents (refer to The chidb File Format document for
 *     the format of cells).
 *
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    // Can't find a cell that doesn't exist
    if(btn->n_cells < ncell || ncell < 0) {
        return CHIDB_ECELLNO;
    }
    
    MemPage* page = btn->page; 
    uint16_t offset = get2byte(((btn->celloffset_array) + ncell*2));  
    
    uint8_t* cell_data = page->data + offset;
    cell->type = btn->type;
    
    if(cell->type == PGTYPE_TABLE_INTERNAL) {
        // Reading a table internal cell, parse child and key
        // Read child page
        uint32_t child_page = get4byte(cell_data + TABLEINTCELL_CHILD_OFFSET);
        cell->fields.tableInternal.child_page = child_page;

        // Read key
        uint32_t key;
        getVarint32(cell_data + TABLEINTCELL_KEY_OFFSET, &key);
        cell->key = key;

    } else if (cell->type == PGTYPE_TABLE_LEAF) {
        // reading a table leaf cell, parse size, key, and data
        // Read data size
        uint32_t data_size;
        getVarint32(cell_data + TABLELEAFCELL_SIZE_OFFSET, &data_size);
        cell->fields.tableLeaf.data_size = data_size; 

        // Read key
        uint32_t key;
        getVarint32(cell_data + TABLELEAFCELL_KEY_OFFSET, &key);
        cell->key = key;
        
        // set data
        cell->fields.tableLeaf.data = cell_data + TABLELEAFCELL_DATA_OFFSET;
    } else if(cell->type == PGTYPE_INDEX_INTERNAL) {
        uint32_t child_page = get4byte(cell_data + INDEXINTCELL_CHILD_OFFSET);
        cell->fields.indexInternal.child_page = child_page;

        cell->key = get4byte(cell_data + INDEXINTCELL_KEYIDX_OFFSET);
        cell->fields.indexInternal.keyPk = get4byte(cell_data + INDEXINTCELL_KEYPK_OFFSET);
    } else {
        cell->key = get4byte(cell_data + INDEXLEAFCELL_KEYIDX_OFFSET);
        cell->fields.indexLeaf.keyPk = get4byte(cell_data + INDEXLEAFCELL_KEYPK_OFFSET);
    }


    return CHIDB_OK;
}


/* Insert a new cell into a B-Tree node
 *
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *  1. Add the cell at the top of the cell area. This involves "translating"
 *     the BTreeCell into the chidb format (refer to The chidb File Format
 *     document for the format of cells).
 *  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *  3. Modify the cell offset array so that all values in positions >= ncell
 *     are shifted one position forward in the array. Then, set the value of
 *     position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    MemPage* page = btn->page;
    uint8_t* data = NULL;
    uint8_t index_magic[] = {0x0B, 0x03, 0x04, 0x04};
    int length;
    
    // Update cell count
    btn->n_cells++;
    // Parse cell into data
    if(cell->type == PGTYPE_TABLE_INTERNAL) {
        // internal table node
        length = TABLEINTCELL_SIZE;
        data = page->data + btn->cells_offset - length;
        put4byte((unsigned char*) (data + TABLEINTCELL_CHILD_OFFSET), 
            cell->fields.tableInternal.child_page);
        putVarint32((unsigned char*) (data + TABLEINTCELL_KEY_OFFSET),
            cell->key);
    } else if(cell->type == PGTYPE_TABLE_LEAF) {
        // table leaf node
        uint32_t size = cell->fields.tableLeaf.data_size;
        length = size + TABLELEAFCELL_SIZE_WITHOUTDATA;
        data = page->data + btn->cells_offset - length;
        
        putVarint32(data + TABLELEAFCELL_SIZE_OFFSET, size);
        putVarint32(data + TABLELEAFCELL_KEY_OFFSET, cell->key);
        memcpy(data + TABLELEAFCELL_DATA_OFFSET, cell->fields.tableLeaf.data, size);
    } else if (cell->type == PGTYPE_INDEX_INTERNAL) {
        // index internal node       
        length = INDEXINTCELL_SIZE;
        data = page->data + btn->cells_offset - length;
        put4byte(data + INDEXINTCELL_CHILD_OFFSET, 
            cell->fields.indexInternal.child_page);
        memcpy(data + INDEXINTCELL_MAGIC_OFFSET,
            index_magic, 4);
        put4byte(data + INDEXINTCELL_KEYIDX_OFFSET, 
            cell->key);
        put4byte(data + INDEXINTCELL_KEYPK_OFFSET, 
            cell->fields.indexInternal.keyPk);
    } else {
        length = INDEXLEAFCELL_SIZE;
        data = page->data + btn->cells_offset - length;
        memcpy(data + INDEXLEAFCELL_MAGIC_OFFSET,
            index_magic, 4);
        put4byte(data + INDEXLEAFCELL_KEYIDX_OFFSET, 
            cell->key);
        put4byte(data + INDEXLEAFCELL_KEYPK_OFFSET, 
            cell->fields.indexLeaf.keyPk);
    }
    
    // Write data into cell area and update offset
    btn->cells_offset -= length;
    
    // Add to cell offset array
    uint8_t* offset_newcell = btn->celloffset_array + (2 * ncell);
   
    // Shift everything past our new cell offset over
    memmove(offset_newcell + 2, offset_newcell, 
        (page->data + btn->free_offset - offset_newcell));
    
    put2byte((unsigned char*) offset_newcell, btn->cells_offset);
    btn->free_offset += 2;

    return CHIDB_OK;
}

/* Find an entry in a table B-Tree
 *
 * Finds the data associated for a given key in a table B-Tree
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Out-parameter where the number of bytes of data must be stored
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key way found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t **data, uint16_t *size)
{
    int err;
    BTreeNode* node;

    if((err = chidb_Btree_getNodeByPage(bt, nroot, &node)) != CHIDB_OK) {
        return err;
    }
    
    for(ncell_t i = 0; i < node->n_cells; i++) {
        BTreeCell cell;
        chidb_Btree_getCell(node, i, &cell);
        if(cell.key == key) {
           // We found it, or at least we're almost there
            if(node->type == PGTYPE_TABLE_LEAF) {
                *size = cell.fields.tableLeaf.data_size;
                *data = (uint8_t *) malloc(sizeof(uint8_t) * (*size));
                
                if(*data == NULL) {
                    chidb_Btree_freeMemNode(bt, node);
                    return CHIDB_ENOMEM;
                }

                memcpy(*data, cell.fields.tableLeaf.data, *size);
                return chidb_Btree_freeMemNode(bt, node);
            }
        }
        if (key <= cell.key){
            // It's lower down the tree (if that exists), so recurse
            if(node->type == PGTYPE_TABLE_LEAF) {
                chidb_Btree_freeMemNode(bt, node);
                return CHIDB_ENOTFOUND;
            }
            chidb_Btree_freeMemNode(bt, node);
            return chidb_Btree_find(bt, 
                cell.fields.tableInternal.child_page, key, data, size);
        }
    }
    // If we made it this far, it either doesn't exist, or is in the right page
    if(node->type == PGTYPE_TABLE_LEAF) {
        chidb_Btree_freeMemNode(bt, node);
        return CHIDB_ENOTFOUND;
    }
    npage_t page = node->right_page;
    chidb_Btree_freeMemNode(bt, node);
    return chidb_Btree_find(bt, page, key, data, size);

}



/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t *data, uint16_t size)
{
    BTreeCell cell;
    
    cell.type = PGTYPE_TABLE_LEAF;
    cell.key = key;
    cell.fields.tableLeaf.data_size = size;
    cell.fields.tableLeaf.data = data;

    int err = chidb_Btree_insert(bt, nroot, &cell);
    return err;
}


/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, chidb_key_t keyIdx, chidb_key_t keyPk)
{
    BTreeCell cell;
    
    cell.type = PGTYPE_INDEX_LEAF;
    cell.key = keyIdx;
    cell.fields.indexInternal.keyPk = keyPk;

    return chidb_Btree_insert(bt, nroot, &cell);
}

// helper function to check if a node can fit a certain cell
// uses pointers to avoid making copy
bool would_overflow(BTreeNode* node, BTreeCell* cell) {
    uint16_t available = node->cells_offset - node->free_offset;

    uint16_t size_cell = 0;
    if(cell->type == PGTYPE_TABLE_INTERNAL) {
        size_cell = TABLEINTCELL_SIZE;
    } else if (cell->type == PGTYPE_TABLE_LEAF) {
        size_cell = TABLELEAFCELL_SIZE_WITHOUTDATA + cell->fields.tableLeaf.data_size;
    } else if (cell->type == PGTYPE_INDEX_INTERNAL) {
        size_cell = INDEXINTCELL_SIZE;
    } else {
        size_cell = INDEXLEAFCELL_SIZE;
    }

    return (size_cell > available);
}

/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
    int err;
    BTreeNode* root;
    check_fail(chidb_Btree_getNodeByPage(bt, nroot, &root));

    // If root is full
    if(would_overflow(root, btc)) {
        // We need to move all of roots contents into a new node
        // And we need to make that node its right page

        // First, make a new node
        BTreeNode* new_right;
        npage_t new_right_num;
        check_fail(chidb_Btree_newNode(bt, &new_right_num, root->type));
        check_fail(chidb_Btree_getNodeByPage(bt, new_right_num, &new_right));

        for(ncell_t i = 0; i < root->n_cells; i++) {
            BTreeCell temp;
            check_fail(chidb_Btree_getCell(root, i, &temp));
            check_fail(chidb_Btree_insertCell(new_right, i, &temp));
        }

        // Empty root and reinit it as an internal node
        check_fail(chidb_Btree_freeMemNode(bt, root));

        // Roots old right page becomes new nodes right page
        new_right->right_page = root->right_page;
        if(new_right->type == PGTYPE_TABLE_LEAF || new_right->type == PGTYPE_TABLE_INTERNAL) {
            // reinit as a table internal
            check_fail(chidb_Btree_initEmptyNode(bt, nroot, PGTYPE_TABLE_INTERNAL));
        } else {
            // reinit as an index internal
            check_fail(chidb_Btree_initEmptyNode(bt, nroot, PGTYPE_INDEX_INTERNAL));
        }

        // Open root again and set it's right page
        check_fail(chidb_Btree_getNodeByPage(bt, nroot, &root));
        root->right_page = new_right_num;

        check_fail(chidb_Btree_writeNode(bt, root));
        check_fail(chidb_Btree_writeNode(bt, new_right));

        check_fail(chidb_Btree_freeMemNode(bt, root));
        check_fail(chidb_Btree_freeMemNode(bt, new_right));

        // Split the new node
        npage_t other_child; // throwaway
        check_fail(chidb_Btree_split(bt, nroot, new_right_num, 0, &other_child));
        
        // Write and close root and new child
    }  

    return chidb_Btree_insertNonFull(bt, nroot, btc);

}


/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
    int err;
    bool found = false;

    BTreeNode* node;
    if((err = chidb_Btree_getNodeByPage(bt, npage, &node)) != CHIDB_OK) {
        return err;
    }

    chidb_key_t key = btc->key;
    BTreeCell search_cell;
    if(node->type == PGTYPE_TABLE_LEAF || node->type == PGTYPE_INDEX_LEAF) { 
        int i;
        for(i = 0; i < node->n_cells; i++) {
            chidb_Btree_getCell(node, i, &search_cell);
            if(key < search_cell.key) {
                break;
            }
            else if (search_cell.key == key)
                return CHIDB_EDUPLICATE;
        }
       
        chidb_Btree_insertCell(node, i, btc); 
        if((err = chidb_Btree_writeNode(bt, node)) != CHIDB_OK) {
            return err;
        }
        return CHIDB_OK;
    } else {
        int i; 
        for(i = 0; i < node->n_cells; i++) {
            chidb_Btree_getCell(node, i, &search_cell);
            if(search_cell.key > key) {
                found = true;
                break;
            }
            else if (search_cell.key == key)
                return CHIDB_EDUPLICATE;
        }
        
        if(!found) {
            npage_t right_page = node->right_page;
            
            check_fail(chidb_Btree_freeMemNode(bt, node));

            BTreeNode* right_page_node;
            check_fail(chidb_Btree_getNodeByPage(bt, right_page, &right_page_node));

            if(would_overflow(right_page_node, btc)) {
                npage_t new_node;

                check_fail(chidb_Btree_freeMemNode(bt, right_page_node));
                
                check_fail(chidb_Btree_split(bt, npage, right_page, i, &new_node));

                return chidb_Btree_insertNonFull(bt, npage, btc);
            }
            return chidb_Btree_insertNonFull(bt, right_page, btc);
        }
        
        npage_t child_num;
        if(node->type == PGTYPE_TABLE_INTERNAL) {
            child_num = search_cell.fields.tableInternal.child_page;
        } else {
            child_num = search_cell.fields.indexInternal.child_page;
        }
        
        BTreeNode* child_node;
        // close our current node
        check_fail(chidb_Btree_freeMemNode(bt, node));
        check_fail(chidb_Btree_getNodeByPage(bt, child_num, &child_node));
        
        if(would_overflow(child_node, btc)) {
            npage_t new_node; // We don't use this, but we need to give it
            // If the child would overflow, split it
            check_fail(chidb_Btree_split(bt, npage, child_num, i, &new_node));
            return chidb_Btree_insertNonFull(bt, npage, btc);
        }

        return chidb_Btree_insertNonFull(bt, child_num, btc);
    }
    
    
    return CHIDB_OK;
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *   cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *   internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *                 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_ncell, npage_t *npage_child2)
{
    int err;
    // Get parent and child nodes
    BTreeNode* parent;
    check_fail(chidb_Btree_getNodeByPage(bt, npage_parent, &parent));

    BTreeNode* child;
    check_fail(chidb_Btree_getNodeByPage(bt, npage_child, &child));

    // Step 1: find median
    ncell_t median = child->n_cells/2; 

    // Step 2: Create new node
    
    // 2a: Get page number of new node in npage_child2
    if((err = chidb_Btree_newNode(bt, npage_child2, child->type)) != CHIDB_OK) {
        return err;
    }
    
    // 2b: Load in new node 
    BTreeNode* new_node;
    if((err = chidb_Btree_getNodeByPage(bt, *npage_child2, &new_node)) != CHIDB_OK) {
        return err;
    }
    
    // Step 3: Move the cells before median
    BTreeCell temp_cell;
    ncell_t top_half = 0; // once moved, we need to reset the child cells top half
    for(ncell_t i = 0; i < median; i++, top_half++) {
        // get
        check_fail(chidb_Btree_getCell(child, i, &temp_cell));
        // insert
        check_fail(chidb_Btree_insertCell(new_node, i, &temp_cell)); 
    }

    // Step 3b: Move median node if neccesary, store it nonetheless
    BTreeCell median_cell;
    check_fail(chidb_Btree_getCell(child, median, &median_cell));
    if(child->type == PGTYPE_TABLE_LEAF) {
        // insert
        check_fail(chidb_Btree_insertCell(new_node, median, &median_cell));
        top_half++;
    }
     


    // We need to change the right_page to the child of the last cell we moved
    // the last cell we moved is stored in median_cell
    if(child->type == PGTYPE_TABLE_INTERNAL) {
        new_node->right_page = median_cell.fields.tableInternal.child_page;
    }
    
    if(child->type == PGTYPE_INDEX_INTERNAL) {
        new_node->right_page = median_cell.fields.indexInternal.child_page;
    }

    // Step 3c: set original child to top half
    
    // First we need to store the top half somewhere
    BTreeNode* top;
    npage_t top_page;
    
    // Create temp top node
    check_fail(chidb_Btree_newNode(bt, &top_page, child->type));
    check_fail(chidb_Btree_getNodeByPage(bt, top_page, &top));


    // Copy top half into top
    for(ncell_t i = top_half; i < child->n_cells; i++) {
        BTreeCell to_insert;
        // get
        check_fail(chidb_Btree_getCell(child, i, &to_insert));

        // insert
        check_fail(chidb_Btree_insertCell(top, i - top_half, &to_insert));
    }
    


    // Now we reset the child
    // (this is only needed because we have no method to delete cells)
    npage_t child_right = child->right_page; // Need to keep right cell as well

    check_fail(chidb_Btree_freeMemNode(bt, child)); // "empty" the child
    check_fail(chidb_Btree_initEmptyNode(bt, npage_child, top->type)); // "recreate" it
    check_fail(chidb_Btree_getNodeByPage(bt, npage_child, &child));


    child->right_page = child_right;
    
    // fill it with top half
    for(ncell_t i = 0; i < top->n_cells; i++) {
        BTreeCell to_insert;

        check_fail(chidb_Btree_getCell(top, i, &to_insert));
        check_fail(chidb_Btree_insertCell(child, i, &to_insert)); 
    }
    
    
    // Step 4: move median cell into parent
    // Step 4a: reassign type
    if(median_cell.type == PGTYPE_INDEX_LEAF) { // can't be leaf, so we must convert
        median_cell.fields.indexInternal.keyPk = 
            median_cell.fields.indexLeaf.keyPk;
    }
    median_cell.type = parent->type;

    // step 4b: reassign child
    if(median_cell.type == PGTYPE_TABLE_INTERNAL) {
        median_cell.fields.tableInternal.child_page = *npage_child2;
    } else if (median_cell.type == PGTYPE_INDEX_INTERNAL) {
        median_cell.fields.indexInternal.child_page = *npage_child2;
    }
       
    // Step 4c: Insert into parent
    check_fail(chidb_Btree_insertCell(parent, parent_ncell, &median_cell));

    // Step 5: Clean up
    // First, erase any sign of our temp node, top
    check_fail(chidb_Btree_freeMemNode(bt, top));
    bt->pager->n_pages--;
    
    // write nodes to disk
    check_fail(chidb_Btree_writeNode(bt, parent));
    check_fail(chidb_Btree_writeNode(bt, child));
    check_fail(chidb_Btree_writeNode(bt, new_node));

    // delete nodes
    check_fail(chidb_Btree_freeMemNode(bt, parent));
    check_fail(chidb_Btree_freeMemNode(bt, child));
    check_fail(chidb_Btree_freeMemNode(bt, new_node));
    

    return CHIDB_OK;
}


