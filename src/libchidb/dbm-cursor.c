/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors
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


#include "dbm-cursor.h"

/* Creates a new trail node for the cursor
 * tree: the tree that this trail is for
 * page: the page that this node is contained in
 */
int chidb_dbm_trail_node_new(BTree* tree, npage_t page, chidb_dbm_trail_node_t** trail_node)
{
    int err;
    
    *trail_node= malloc(sizeof(chidb_dbm_trail_node_t));
    if(*trail_node == NULL)
        return CHIDB_ENOMEM;
    // Load the page in
    BTreeNode* node;
    check_fail(chidb_Btree_getNodeByPage(tree, page, &node));
    
    (*trail_node)->node = node;
    (*trail_node)->cell_num = 0;
    return CHIDB_OK;
}

int chidb_dbm_cursor_new(BTree* tree, npage_t root, chidb_dbm_cursor_t* cursor)
{   
    // Initialize linked list for trail
    list_init(&cursor->root_trail);
    
    // Init first trail node
    chidb_dbm_trail_node_t* trail_node;
    chidb_dbm_trail_node_new(tree, root, &trail_node);

    // Insert first node
    list_insert_at(&cursor->root_trail, trail_node, 0);
    
    cursor->root_page = root;

    return CHIDB_OK;
}

int chidb_dbm_cursor_rewind(BTree* tree, chidb_dbm_cursor_t* cursor) 
{
    // destroy our trail
    list_destroy(&cursor->root_trail); 
    list_init(&cursor->root_trail);
 
    chidb_dbm_trail_node_t* trail_node;
    chidb_dbm_trail_node_new(tree, cursor->root_page, &trail_node);

    list_append(&cursor->root_trail, trail_node);
    
    chidb_dbm_cursor_table_down(tree, cursor, true);

    return CHIDB_OK;
}

int chidb_dbm_cursor_table_down(BTree* tree, chidb_dbm_cursor_t* cursor, bool forward)
{
    uint32_t end = list_size(&cursor->root_trail) - 1; 
    chidb_dbm_trail_node_t* trail_node = list_get_at(&cursor->root_trail, end);
    if(trail_node->node->type == PGTYPE_TABLE_LEAF) {
        // Place the cell into the cursor and return
        chidb_Btree_getCell(trail_node->node, trail_node->cell_num, &cursor->cell);
        return CHIDB_OK;
    }

    npage_t next_page;
    if(trail_node->cell_num < trail_node->node->n_cells) {
        BTreeCell cell;
        chidb_Btree_getCell(trail_node->node, trail_node->cell_num, &cell);
        next_page = cell.fields.tableInternal.child_page;
    } else { // unless something has gone wrong, this means we need to go down to right_page
        next_page = trail_node->node->right_page;
    }

    chidb_dbm_trail_node_t* next_trail_node;
    chidb_dbm_trail_node_new(tree, next_page, &next_trail_node);

    if(!forward)
        next_trail_node->cell_num = next_trail_node->node->n_cells;
    
    list_append(&cursor->root_trail, next_trail_node);
    return chidb_dbm_cursor_table_down(tree, cursor, forward);
}

/*
 * We assume either seek or rewind has been called, therefore asserting that the node type is a Leaf
 * If the node type is not a Leaf, something has gone horribly wrong
 */
int chidb_dbm_cursor_table_move(BTree* tree, chidb_dbm_cursor_t* cursor, bool forward) 
{
    int err;

    // Get last object on trail, read the next child or go onto right page/child
    uint32_t last = list_size(&cursor->root_trail) - 1;
    chidb_dbm_trail_node_t* trail_node = list_get_at(&cursor->root_trail, last);
    
    // Get our cell
    BTreeCell cell;
    BTreeNode* node = trail_node->node;
    
    bool up = false;
    if(forward) {
       up = trail_node->cell_num == trail_node->node->n_cells - 1; 
    } else {
        up = trail_node->cell_num == 0;
    }

    if(up) {
        // Last cell
        list_delete_at(&cursor->root_trail, last); // Delete current node, we're moving
        return chidb_dbm_cursor_table_up(tree, cursor, forward);
    }

    if(forward)
        trail_node->cell_num++;
    else
        trail_node->cell_num--;
    check_fail(chidb_Btree_getCell(node, trail_node->cell_num, &cell));
    
    cursor->cell = cell;

    return CHIDB_OK;
}


int chidb_dbm_cursor_table_up(BTree* tree, chidb_dbm_cursor_t* cursor, bool forward)
{
    // We assume current node on trail is the one above where we were
    uint32_t last = list_size(&cursor->root_trail) - 1;

    // This means we are at the root and trying to go further up
    if(last == -1)
        return CHIDB_CANTMOVE;

    chidb_dbm_trail_node_t* trail_node = list_get_at(&cursor->root_trail, last);

    if(forward)
        trail_node->cell_num++; // move onto next cell
    else
        trail_node->cell_num--;

    bool down = false; 

    if(forward)
        down = trail_node->cell_num <= trail_node->node->n_cells;
    else
        down = trail_node->cell_num >= 0;

    if(down) {
        // We can head down to child or right_page
        return chidb_dbm_cursor_table_down(tree, cursor, forward);
    } else {
        // We have passed through all children, we must go up again
        list_delete_at(&cursor->root_trail, last);
        return chidb_dbm_cursor_table_up(tree, cursor, forward);
    }

    return CHIDB_OK;
}
