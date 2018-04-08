/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors -- header
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


#ifndef DBM_CURSOR_H_
#define DBM_CURSOR_H_

#include "chidbInt.h"
#include "btree.h"
#include "simclist.h"

typedef enum chidb_dbm_cursor_type
{
    CURSOR_UNSPECIFIED,
    CURSOR_READ,
    CURSOR_WRITE
} chidb_dbm_cursor_type_t;

// This struct will store a BTreeNode and information about it
// We can use a list of these to hold a trail back to the root
typedef struct chidb_dbm_trail_node
{
    BTreeNode* node;
    ncell_t cell_num;

} chidb_dbm_trail_node_t;

typedef struct chidb_dbm_cursor
{
    chidb_dbm_cursor_type_t type;
   
    npage_t root_page; // for rewinding
    list_t root_trail; // a list back to the root
    BTreeCell cell; // The current cell

} chidb_dbm_cursor_t;

/* Trail functions */
int chidb_dbm_trail_node_new(BTree* tree, npage_t page, chidb_dbm_trail_node_t** node);

/* Cursor function definitions go here */
int chidb_dbm_cursor_new(BTree* tree, npage_t root, chidb_dbm_cursor_t* cursor);

int chidb_dbm_cursor_rewind(BTree* tree, chidb_dbm_cursor_t* cursor);

int chidb_dbm_cursor_table_move(BTree* tree, chidb_dbm_cursor_t* cursor, bool forward);

int chidb_dbm_cursor_table_up(BTree* tree, chidb_dbm_cursor_t* cursor, bool forward);

int chidb_dbm_cursor_table_down(BTree* tree, chidb_dbm_cursor_t* cursor, bool forward);


#endif /* DBM_CURSOR_H_ */
