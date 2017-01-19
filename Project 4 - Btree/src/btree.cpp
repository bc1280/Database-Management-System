/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // construct an index name
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();
    
    // initialize and set attributes
    outIndexName = indexName;
    this->bufMgr = bufMgrIn;
    this->attrByteOffset = attrByteOffset;
    this->attributeType = attrType;
    this->scanExecuting = false;
    this->leafOccupancy = INTARRAYLEAFSIZE;
    this->nodeOccupancy = INTARRAYNONLEAFSIZE;

    
    if(File::exists(indexName)) {
        // if the index file exists, the file is opened
        this->file = new BlobFile(indexName, false);
        Page* metaPage;
        IndexMetaInfo * metaInfo;
        // Read meta info page
        this->headerPageNum = file->getFirstPageNo();
        this->bufMgr->readPage(this->file, this->headerPageNum, metaPage);
        metaInfo = (IndexMetaInfo *) metaPage;
        // Save attributes
        if(this->attrByteOffset == metaInfo->attrByteOffset && this->attributeType == metaInfo->attrType) {
            this->rootPageNum = metaInfo->rootPageNo;
        } else {
            throw BadIndexInfoException;
        }
        
	rootPageNum = metaInfo->rootPageNo;
        rootIsLeaf = (metaInfo->rootPageNo == 2);
        this->bufMgr->unPinPage(file, headerPageNum, false);
        
        
    } else {
        // else, a new index file is created
        this->file = new BlobFile(indexName, true);
        Page* metaPage; // header page that stores struct IndexMetaInfo
	Page* rootPage; // root of Btree
	IndexMetaInfo * meta;
	// Save attributes
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	rootIsLeaf = true; // root node is initially a LeafNode

	// allocate metaInfo page, allocate root page
	this->bufMgr->allocPage(this->file, this->headerPageNum, metaPage);
	this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);

	meta = (IndexMetaInfo *) metaPage;

	meta->attrByteOffset = this->attrByteOffset;
	meta->attrType = this->attributeType;
	meta->rootPageNo = this->rootPageNum;
	strcpy(meta->relationName, relationName.c_str());

	// Cast rootPage to LeafNode (root is a leaf for a new Btree)
        LeafNodeInt* root = (LeafNodeInt*)rootPage;
	root->rightSibPageNo = 0;


	this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
	this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

	// Scan the relation file
	FileScan* scan = new FileScan(relationName, this->bufMgr);
	try {
		while (true) {
			std::string recordString;
			RecordId rid;

			// Iterate the records in relation file
			scan->scanNext(rid);
			recordString = scan->getRecord();
			void* key = (void*)(recordString.c_str()+attrByteOffset);
			insertEntry(key, rid);
		}
		
	}
	catch (EndOfFileException e) {}

	this->bufMgr->flushFile(this->file);
	delete scan;
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // clearing up any state variables
    try {
        endScan();
    } catch (ScanNotInitializedException e){}
    this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
    this->bufMgr->flushFile(this->file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // create new RIDKeyPair
	RIDKeyPair<int> entry;
	entry.set(rid, *(int *)key);

	// load root page and declare target node (leaf node that entry shoule be inserted into)
	Page* rootPage;
	PageId rootPageNo = rootPageNum;
	bufMgr->readPage(file, rootPageNo, rootPage);
	Page* targetPage;
	PageId targetPageNo;
	LeafNodeInt* targetNode;

	if(rootIsLeaf) {
		LeafNodeInt * rootNode = (LeafNodeInt *)rootPage;

		if(rootNode->ridArray[leafOccupancy-1].page_number != 0) {
                    // split if root node is full
                    PageId newLeafPageNo;
                    Page * newLeafPage;
                    bufMgr->allocPage(file, newLeafPageNo, newLeafPage);
                    LeafNodeInt * newLeafNode = (LeafNodeInt *)newLeafPage;

                    // initialize new leaf node
                    int mid = leafOccupancy/2; // middle index
                    for(int i = 0; i < mid; i++) {
                        newLeafNode->keyArray[i] = rootNode ->keyArray[mid+i];
                        newLeafNode->ridArray[i] = rootNode ->ridArray[mid+i];
                        rootNode->ridArray[mid+i].page_number = 0;
                    }
                    newLeafNode->rightSibPageNo = rootNode ->rightSibPageNo;
                    rootNode ->rightSibPageNo = newLeafPageNo;

                    if(rootIsLeaf) {
                        // create new root node if leaf node is root node
                        PageId newRootPageNo;
                        Page * newRootPage;
                        bufMgr->allocPage(file, newRootPageNo, newRootPage);
                        NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                        // initialize new root node
                        newRootNode->level = 1;
                        newRootNode->keyArray[0] = newLeafNode->keyArray[0];
                        newRootNode->pageNoArray[0] = rootPageNo;
                        newRootNode->pageNoArray[1] = newLeafPageNo;

                        // update meta page
                        Page * metaPage;
                        bufMgr->readPage(file, headerPageNum, metaPage);
                        IndexMetaInfo * metaInfo = (IndexMetaInfo *)metaPage;
                        metaInfo->rootPageNo = newRootPageNo;

                        rootPageNum = newRootPageNo;
                        rootIsLeaf = false;
                        bufMgr->unPinPage(file, newRootPageNo, true);
                        bufMgr->unPinPage(file, headerPageNum, true);

                    } else {
                        // create new PageKeyPair
                        PageKeyPair<int> entry;
                        entry.set(newLeafPageNo, newLeafNode->keyArray[0]);

                        // push up the middle key
                        Page * parentPage;
                        bufMgr->readPage(file, rootPageNo, parentPage);
                        NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                        // insert non leaf node
                        int pos = nodeOccupancy;

                        // find appropriate position to insert entry
                        while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                            pos--;
                        }
                        while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                            parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                            parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                            pos--;
                        }
                        // insert entry into position
                        parentNode->keyArray[pos] = entry.key;
                        parentNode->pageNoArray[pos+1] = entry.pageNo;
                        bufMgr->unPinPage(file, rootPageNo, true);
                    }

                    bufMgr->unPinPage(file, newLeafPageNo, true);

                    bufMgr->unPinPage(file, rootPageNo, true);
		    // traverse through nodes
                    // initialize start/parent node and current node
                    Page * parentPage;
                    PageId parentPageNo = rootPageNum;
                    bufMgr->readPage(file, parentPageNo, parentPage);
                    NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                    Page * currentPage;
                    PageId currentPageNo = parentPageNo;
                    NonLeafNodeInt * currentNode = parentNode;

                    int pos = nodeOccupancy; // position in array
                    while(currentNode->level != 1) {
                        // find appropriate position to insert entry
                        while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
                            pos--;
                        }
                        while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
                            pos--;
                        }

                        // load current node to be the next node
                        currentPageNo = currentNode->pageNoArray[pos+1];
                        bufMgr->readPage(file, currentPageNo, currentPage);
                        currentNode = (NonLeafNodeInt *)currentPage;

                        if(currentNode->pageNoArray[nodeOccupancy] != 0) {
                            // if current node is full, split and set current node back to its parent
                            // create new non-leaf node
                            PageId newNonLeafPageNo;
                            Page * newNonLeafPage;
                            bufMgr->allocPage(file, newNonLeafPageNo, newNonLeafPage);
                            NonLeafNodeInt * newNonLeafNode = (NonLeafNodeInt *)newNonLeafPage;

                            // initialize new non-leaf node
                            int mid = nodeOccupancy/2; // middle index
                            for(int i = 0; i < mid; i++) {
                                newNonLeafNode->keyArray[i] = currentNode->keyArray[mid+i];
                                newNonLeafNode->pageNoArray[i] = currentNode->pageNoArray[(mid+1)+i];
                                currentNode->pageNoArray[(mid+1)+i] = 0;
                            }
                            newNonLeafNode->level = currentNode->level;

                            if(currentPageNo == rootPageNum) {
                                // create new root node
                                PageId newRootPageNo;
                                Page * newRootPage;
                                bufMgr->allocPage(file, newRootPageNo, newRootPage);
                                NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                                // initialize new root node
                                newRootNode->level = 0;
                                newRootNode->keyArray[0] = newNonLeafNode->keyArray[0];
                                newRootNode->pageNoArray[0] = currentPageNo;
                                newRootNode->pageNoArray[1] = newNonLeafPageNo;

                                // update meta page
                                Page* metaPage;
                                bufMgr->readPage(file, headerPageNum, metaPage);
                                IndexMetaInfo* metaInfo = (IndexMetaInfo*)metaPage;
                                metaInfo->rootPageNo = newRootPageNo;

                                rootPageNum = newRootPageNo;
                                bufMgr->unPinPage(file, newRootPageNo, true);
                                bufMgr->unPinPage(file, headerPageNum, true);

                            } else {
                                // create new PageKeyPair
                                PageKeyPair<int> entry;
                                entry.set(newNonLeafPageNo, newNonLeafNode->keyArray[0]);

                                // push up the middle key
                                Page* parentPage;
                                bufMgr->readPage(file, parentPageNo, parentPage);
                                NonLeafNodeInt* parentNode = (NonLeafNodeInt*)parentPage;
                                // insert non leaf
                                int pos = nodeOccupancy;
                                // find appropriate position to insert entry
                                while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                                    pos--;
                                }
                                while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                                    parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                                    parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                                    pos--;
                                }
                                // insert entry into position
                                parentNode->keyArray[pos] = entry.key;
                                parentNode->pageNoArray[pos+1] = entry.pageNo;
                                bufMgr->unPinPage(file, parentPageNo, true);
                            }
                            bufMgr->unPinPage(file, newNonLeafPageNo, true);

                            bufMgr->unPinPage(file, currentPageNo, true);
                            currentPageNo = parentPageNo;
                            currentNode = parentNode;

                        } else if(currentNode->level != 1){
                            // update parent node
                            bufMgr->unPinPage(file, parentPageNo, false);
                            parentPageNo = currentPageNo;
                            parentNode = currentNode;

                        } else {
                            bufMgr->unPinPage(file, parentPageNo, false);
                        }
                    }

                    // find appropriate position to insert entry
                    pos = nodeOccupancy;
                    while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
                        pos--;
                    }
                    while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
                        pos--;
                    }

                    // load target node
                    Page * targetPage;
                    PageId targetPageNo = currentNode->pageNoArray[pos];
                    bufMgr->readPage(file, targetPageNo, targetPage);
                    LeafNodeInt * targetNode = (LeafNodeInt *)targetPage;
                    bufMgr->unPinPage(file, currentPageNo, false);

                    if(targetNode->ridArray[leafOccupancy-1].page_number != 0) {
                        // split if the leaf node is full
                        // create new leaf node
                        PageId newLeafPageNo;
                        Page * newLeafPage;
                        bufMgr->allocPage(file, newLeafPageNo, newLeafPage);
                        LeafNodeInt * newLeafNode = (LeafNodeInt *)newLeafPage;

                        // initialize new leaf node
                        int mid = leafOccupancy/2; // middle index
                        for(int i = 0; i < mid; i++) {
                            newLeafNode->keyArray[i] = targetNode->keyArray[mid+i];
                            newLeafNode->ridArray[i] = targetNode->ridArray[mid+i];
                            targetNode->ridArray[mid+i].page_number = 0;
                        }
                        newLeafNode->rightSibPageNo = targetNode->rightSibPageNo;
                        targetNode->rightSibPageNo = newLeafPageNo;

                        if(rootIsLeaf) {
                            // create new root node if leaf node is root node
                            PageId newRootPageNo;
                            Page * newRootPage;
                            bufMgr->allocPage(file, newRootPageNo, newRootPage);
                            NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                            // initialize new root node
                            newRootNode->level = 1;
                            newRootNode->keyArray[0] = newLeafNode->keyArray[0];
                            newRootNode->pageNoArray[0] = targetPageNo;
                            newRootNode->pageNoArray[1] = newLeafPageNo;

                            // update meta page
                            Page * metaPage;
                            bufMgr->readPage(file, headerPageNum, metaPage);
                            IndexMetaInfo * metaInfo = (IndexMetaInfo *)metaPage;
                            metaInfo->rootPageNo = newRootPageNo;
                            rootPageNum = newRootPageNo;
                            rootIsLeaf = false;
                            bufMgr->unPinPage(file, newRootPageNo, true);
                            bufMgr->unPinPage(file, headerPageNum, true);

                        } else {
                            // create new PageKeyPair
                            PageKeyPair<int> entry;
                            entry.set(newLeafPageNo, newLeafNode->keyArray[0]);
                            // push up the middle key
                            Page * parentPage;
                            bufMgr->readPage(file, currentPageNo, parentPage);
                            NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                            // insert non leaf
                            int pos = nodeOccupancy;
                            // find appropriate position to insert entry
                            while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                                pos--;
                            }
                            while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                                parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                                parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                                pos--;
                            }
                            // insert entry into position
                            parentNode->keyArray[pos] = entry.key;
                            parentNode->pageNoArray[pos+1] = entry.pageNo;
                            bufMgr->unPinPage(file, currentPageNo, true);
                        }

                        bufMgr->unPinPage(file, newLeafPageNo, true);
                        bufMgr->unPinPage(file, targetPageNo, true);

                        // find new target node
                        bufMgr->readPage(file, currentPageNo, currentPage);
                        currentNode = (NonLeafNodeInt *)currentPage;
                        pos = nodeOccupancy;
                        while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
                            pos--;
                        }
                        while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
                            pos--;
                        }

                        targetPageNo = currentNode->pageNoArray[pos];
                        bufMgr->unPinPage(file, currentPageNo, false);

                    } else {
                        targetPageNo = targetPageNo;
                        bufMgr->unPinPage(file, targetPageNo, false);
                    }

		} else {
			// set target node to be root node
			targetPageNo = rootPageNo;
			bufMgr->unPinPage(file, rootPageNo, false);
		}

	} else {
		NonLeafNodeInt * rootNode = (NonLeafNodeInt *)rootPage;

		if(rootNode->pageNoArray[nodeOccupancy] != 0) {
			// split if root node is full
                    // create new non-leaf node
                    PageId newNonLeafPageNo;
                    Page * newNonLeafPage;
                    bufMgr->allocPage(file, newNonLeafPageNo, newNonLeafPage);
                    NonLeafNodeInt * newNonLeafNode = (NonLeafNodeInt *)newNonLeafPage;

                    // initialize new non-leaf node
                    int mid = nodeOccupancy/2; // middle index
                    for(int i = 0; i < mid; i++) {
                        newNonLeafNode->keyArray[i] = rootNode->keyArray[mid+i];
                        newNonLeafNode->pageNoArray[i] = rootNode->pageNoArray[(mid+1)+i];
                        rootNode->pageNoArray[(mid+1)+i] = 0;
                    }
                    newNonLeafNode->level = rootNode->level;

                    if(rootPageNo == rootPageNum) {
                        // create new root node
                        PageId newRootPageNo;
                        Page * newRootPage;
                        bufMgr->allocPage(file, newRootPageNo, newRootPage);
                        NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                        // initialize new root node
                        newRootNode->level = 0;
                        newRootNode->keyArray[0] = newNonLeafNode->keyArray[0];
                        newRootNode->pageNoArray[0] = rootPageNo;
                        newRootNode->pageNoArray[1] = newNonLeafPageNo;

                        // update meta page
                        Page* metaPage;
                        bufMgr->readPage(file, headerPageNum, metaPage);
                        IndexMetaInfo* metaInfo = (IndexMetaInfo*)metaPage;
                        metaInfo->rootPageNo = newRootPageNo;

                        rootPageNum = newRootPageNo;
                        bufMgr->unPinPage(file, newRootPageNo, true);
                        bufMgr->unPinPage(file, headerPageNum, true);

                    } else {
                        // create new PageKeyPair
                        PageKeyPair<int> entry;
                        entry.set(newNonLeafPageNo, newNonLeafNode->keyArray[0]);

                        // push up the middle key
                        Page* parentPage;
                        bufMgr->readPage(file, rootPageNo, parentPage);
                        NonLeafNodeInt* parentNode = (NonLeafNodeInt*)parentPage;
                        int pos = nodeOccupancy;
                        // find appropriate position to insert entry
                        while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                            pos--;
                        }
                        while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                            parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                            parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                            pos--;
                        }

                        // insert entry into position
                        parentNode->keyArray[pos] = entry.key;
                        parentNode->pageNoArray[pos+1] = entry.pageNo;
                        bufMgr->unPinPage(file, rootPageNo, true);
                    }

                    bufMgr->unPinPage(file, newNonLeafPageNo, true);
                    bufMgr->unPinPage(file, rootPageNo, true);

		} else {
			bufMgr->unPinPage(file, rootPageNo, false);
		}
                // traverse through nodes
                // initialize start/parent node and current node
                Page * parentPage;
                PageId parentPageNo = rootPageNum;
                bufMgr->readPage(file, parentPageNo, parentPage);
                NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                Page * currentPage;
                PageId currentPageNo = parentPageNo;
                NonLeafNodeInt * currentNode = parentNode;

                int pos = nodeOccupancy; // position in array
                while(currentNode->level != 1) {
                    // find appropriate position to insert entry
                    while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
			pos--;
                    }
                    while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
			pos--;
                    }

                    // load current node to be the next node
                    currentPageNo = currentNode->pageNoArray[pos+1];
                    bufMgr->readPage(file, currentPageNo, currentPage);
                    currentNode = (NonLeafNodeInt *)currentPage;

                    if(currentNode->pageNoArray[nodeOccupancy] != 0) {
                        // if current node is full, split and set current node back to its parent
			// create new non-leaf node
                        PageId newNonLeafPageNo;
                        Page * newNonLeafPage;
                        bufMgr->allocPage(file, newNonLeafPageNo, newNonLeafPage);
                        NonLeafNodeInt * newNonLeafNode = (NonLeafNodeInt *)newNonLeafPage;

                        // initialize new non-leaf node
                        int mid = nodeOccupancy/2; // middle index
                        for(int i = 0; i < mid; i++) {
                            newNonLeafNode->keyArray[i] = currentNode->keyArray[mid+i];
                            newNonLeafNode->pageNoArray[i] = currentNode->pageNoArray[(mid+1)+i];
                            currentNode->pageNoArray[(mid+1)+i] = 0;
                        }
                        newNonLeafNode->level = currentNode->level;

                        if(currentPageNo == rootPageNum) {
                            // create new root node
                            PageId newRootPageNo;
                            Page * newRootPage;
                            bufMgr->allocPage(file, newRootPageNo, newRootPage);
                            NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                            // initialize new root node
                            newRootNode->level = 0;
                            newRootNode->keyArray[0] = newNonLeafNode->keyArray[0];
                            newRootNode->pageNoArray[0] = currentPageNo;
                            newRootNode->pageNoArray[1] = newNonLeafPageNo;

                            // update meta page
                            Page* metaPage;
                            bufMgr->readPage(file, headerPageNum, metaPage);
                            IndexMetaInfo* metaInfo = (IndexMetaInfo*)metaPage;
                            metaInfo->rootPageNo = newRootPageNo;

                            rootPageNum = newRootPageNo;
                            bufMgr->unPinPage(file, newRootPageNo, true);
                            bufMgr->unPinPage(file, headerPageNum, true);

                        } else {
                            // create new PageKeyPair
                            PageKeyPair<int> entry;
                            entry.set(newNonLeafPageNo, newNonLeafNode->keyArray[0]);

                            // push up the middle key
                            Page* parentPage;
                            bufMgr->readPage(file, parentPageNo, parentPage);
                            NonLeafNodeInt* parentNode = (NonLeafNodeInt*)parentPage;
                            // insert non leaf
                            int pos = nodeOccupancy;
                            // find appropriate position to insert entry
                            while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                                pos--;
                            }
                            while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                                parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                                parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                                pos--;
                            }
                            // insert entry into position
                            parentNode->keyArray[pos] = entry.key;
                            parentNode->pageNoArray[pos+1] = entry.pageNo;
                            bufMgr->unPinPage(file, parentPageNo, true);
                        }
                        bufMgr->unPinPage(file, newNonLeafPageNo, true);

			bufMgr->unPinPage(file, currentPageNo, true);
			currentPageNo = parentPageNo;
			currentNode = parentNode;

                    } else if(currentNode->level != 1){
                        // update parent node
			bufMgr->unPinPage(file, parentPageNo, false);
			parentPageNo = currentPageNo;
			parentNode = currentNode;

                    } else {
                            bufMgr->unPinPage(file, parentPageNo, false);
                    }
                }

                // find appropriate position to insert entry
                pos = nodeOccupancy;
                while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
                    pos--;
                }
                while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
                    pos--;
                }

                // load target node
                Page * targetPage;
                PageId targetPageNo = currentNode->pageNoArray[pos];
                bufMgr->readPage(file, targetPageNo, targetPage);
                LeafNodeInt * targetNode = (LeafNodeInt *)targetPage;
                bufMgr->unPinPage(file, currentPageNo, false);

                if(targetNode->ridArray[leafOccupancy-1].page_number != 0) {
                    // split if the leaf node is full
                    // create new leaf node
                    PageId newLeafPageNo;
                    Page * newLeafPage;
                    bufMgr->allocPage(file, newLeafPageNo, newLeafPage);
                    LeafNodeInt * newLeafNode = (LeafNodeInt *)newLeafPage;

                    // initialize new leaf node
                    int mid = leafOccupancy/2; // middle index
                    for(int i = 0; i < mid; i++) {
                        newLeafNode->keyArray[i] = targetNode->keyArray[mid+i];
                        newLeafNode->ridArray[i] = targetNode->ridArray[mid+i];
                        targetNode->ridArray[mid+i].page_number = 0;
                    }
                    newLeafNode->rightSibPageNo = targetNode->rightSibPageNo;
                    targetNode->rightSibPageNo = newLeafPageNo;

                    if(rootIsLeaf) {
                        // create new root node if leaf node is root node
                        PageId newRootPageNo;
                        Page * newRootPage;
                        bufMgr->allocPage(file, newRootPageNo, newRootPage);
                        NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                        // initialize new root node
                        newRootNode->level = 1;
                        newRootNode->keyArray[0] = newLeafNode->keyArray[0];
                        newRootNode->pageNoArray[0] = targetPageNo;
                        newRootNode->pageNoArray[1] = newLeafPageNo;

                        // update meta page
                        Page * metaPage;
                        bufMgr->readPage(file, headerPageNum, metaPage);
                	IndexMetaInfo * metaInfo = (IndexMetaInfo *)metaPage;
                        metaInfo->rootPageNo = newRootPageNo;
                        rootPageNum = newRootPageNo;
                        rootIsLeaf = false;
                        bufMgr->unPinPage(file, newRootPageNo, true);
                        bufMgr->unPinPage(file, headerPageNum, true);

                    } else {
                        // create new PageKeyPair
                        PageKeyPair<int> entry;
                        entry.set(newLeafPageNo, newLeafNode->keyArray[0]);
                        // push up the middle key
                        Page * parentPage;
                        bufMgr->readPage(file, currentPageNo, parentPage);
                        NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                        // insert non leaf
                        int pos = nodeOccupancy;
                        // find appropriate position to insert entry
                        while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                            pos--;
                        }
                        while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                            parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                            parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                            pos--;
                        }
                        // insert entry into position
                        parentNode->keyArray[pos] = entry.key;
                        parentNode->pageNoArray[pos+1] = entry.pageNo;
                        bufMgr->unPinPage(file, currentPageNo, true);
                    }

                    bufMgr->unPinPage(file, newLeafPageNo, true);
                    bufMgr->unPinPage(file, targetPageNo, true);

                    // find new target node
                    bufMgr->readPage(file, currentPageNo, currentPage);
                    currentNode = (NonLeafNodeInt *)currentPage;
                    pos = nodeOccupancy;
                    while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
			pos--;
                    }
                    while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
			pos--;
                    }

                    targetPageNo = currentNode->pageNoArray[pos];
                    bufMgr->unPinPage(file, currentPageNo, false);

                } else {
                    targetPageNo = targetPageNo;
                    bufMgr->unPinPage(file, targetPageNo, false);
                }
	}

	// insert entry into target node
	bufMgr->readPage(file, targetPageNo, targetPage);
	targetNode = (LeafNodeInt *)targetPage;
	if(targetNode->ridArray[0].page_number == 0) {
		// insert entry at index 0 if array is empty
		targetNode->keyArray[0] = entry.key;
		targetNode->ridArray[0] = entry.rid;

	} else {
		int pos = leafOccupancy-1;

		// find appropriate position to insert entry
		while((pos >= 0) && (targetNode->ridArray[pos].page_number == 0)) {
			pos--;
		}
		while((pos >= 0) && (targetNode->keyArray[pos] > entry.key)) {
			targetNode->keyArray[pos+1] = targetNode->keyArray[pos];
			targetNode->ridArray[pos+1] = targetNode->ridArray[pos];
			pos--;
		}

		// insert entry into position
		targetNode->keyArray[pos+1] = entry.key;
		targetNode->ridArray[pos+1] = entry.rid;
	}
	bufMgr->unPinPage(file, targetPageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm) {
        // set fields
	lowValInt = *(int *)lowValParm;
	highValInt = *(int *)highValParm;
	lowOp = lowOpParm;
	highOp = highOpParm;

        // check range
	if (lowValInt > highValInt) {
		throw BadScanrangeException();
	}
	if (lowOpParm != GTE && lowOpParm != GT) {
		throw BadOpcodesException();
	}
	if (highOpParm != LTE && highOpParm != LT) {
		throw BadOpcodesException();
	}

	if(scanExecuting){
		endScan();
	}
	scanExecuting = true;

	LeafNodeInt * currentNode;
	if(rootIsLeaf) {
		currentPageNum = rootPageNum;
		bufMgr->readPage(file, currentPageNum, currentPageData);

	} else {
		RIDKeyPair<int> entry;
		RecordId rid;
		entry.set(rid, lowValInt);
		Page * parentPage;
                PageId parentPageNo = rootPageNum;
                bufMgr->readPage(file, parentPageNo, parentPage);
                NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                Page * currentPage;
                PageId currentPageNo = parentPageNo;
                NonLeafNodeInt * currentNode = parentNode;

                int pos = nodeOccupancy; // position in array
                while(currentNode->level != 1) {
                    // find appropriate position to insert entry
                    while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
			pos--;
                    }
                    while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
			pos--;
                    }

                    // load current node to be the next node
                    currentPageNo = currentNode->pageNoArray[pos+1];
                    bufMgr->readPage(file, currentPageNo, currentPage);
                    currentNode = (NonLeafNodeInt *)currentPage;

                    if(currentNode->pageNoArray[nodeOccupancy] != 0) {
                        // if current node is full, split and set current node back to its parent
			// create new non-leaf node
                        PageId newNonLeafPageNo;
                        Page * newNonLeafPage;
                        bufMgr->allocPage(file, newNonLeafPageNo, newNonLeafPage);
                        NonLeafNodeInt * newNonLeafNode = (NonLeafNodeInt *)newNonLeafPage;

                        // initialize new non-leaf node
                        int mid = nodeOccupancy/2; // middle index
                        for(int i = 0; i < mid; i++) {
                            newNonLeafNode->keyArray[i] = currentNode->keyArray[mid+i];
                            newNonLeafNode->pageNoArray[i] = currentNode->pageNoArray[(mid+1)+i];
                            currentNode->pageNoArray[(mid+1)+i] = 0;
                        }
                        newNonLeafNode->level = currentNode->level;

                        if(currentPageNo == rootPageNum) {
                            // create new root node
                            PageId newRootPageNo;
                            Page * newRootPage;
                            bufMgr->allocPage(file, newRootPageNo, newRootPage);
                            NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                            // initialize new root node
                            newRootNode->level = 0;
                            newRootNode->keyArray[0] = newNonLeafNode->keyArray[0];
                            newRootNode->pageNoArray[0] = currentPageNo;
                            newRootNode->pageNoArray[1] = newNonLeafPageNo;

                            // update meta page
                            Page* metaPage;
                            bufMgr->readPage(file, headerPageNum, metaPage);
                            IndexMetaInfo* metaInfo = (IndexMetaInfo*)metaPage;
                            metaInfo->rootPageNo = newRootPageNo;

                            rootPageNum = newRootPageNo;
                            bufMgr->unPinPage(file, newRootPageNo, true);
                            bufMgr->unPinPage(file, headerPageNum, true);

                        } else {
                            // create new PageKeyPair
                            PageKeyPair<int> entry;
                            entry.set(newNonLeafPageNo, newNonLeafNode->keyArray[0]);

                            // push up the middle key
                            Page* parentPage;
                            bufMgr->readPage(file, parentPageNo, parentPage);
                            NonLeafNodeInt* parentNode = (NonLeafNodeInt*)parentPage;
                            int pos = nodeOccupancy;
                            // find appropriate position to insert entry
                            while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                                pos--;
                            }
                            while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                                parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                                parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                                pos--;
                            }

                            // insert entry into position
                            parentNode->keyArray[pos] = entry.key;
                            parentNode->pageNoArray[pos+1] = entry.pageNo;
                            bufMgr->unPinPage(file, parentPageNo, true);
                        }

                        bufMgr->unPinPage(file, newNonLeafPageNo, true);
			bufMgr->unPinPage(file, currentPageNo, true);
			currentPageNo = parentPageNo;
			currentNode = parentNode;

                    } else if(currentNode->level != 1){
                        // update parent node
			bufMgr->unPinPage(file, parentPageNo, false);
			parentPageNo = currentPageNo;
			parentNode = currentNode;

                    } else {
			bufMgr->unPinPage(file, parentPageNo, false);
                    }
                }

                // find appropriate position to insert entry
                pos = nodeOccupancy;
                while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
                    pos--;
                }
                while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
                    pos--;
                }

                // load target node
                Page * targetPage;
                PageId targetPageNo = currentNode->pageNoArray[pos];
                bufMgr->readPage(file, targetPageNo, targetPage);
                LeafNodeInt * targetNode = (LeafNodeInt *)targetPage;
                bufMgr->unPinPage(file, currentPageNo, false);

                if(targetNode->ridArray[leafOccupancy-1].page_number != 0) {
                    // split if the leaf node is full
                    // create new leaf node
                    PageId newLeafPageNo;
                    Page * newLeafPage;
                    bufMgr->allocPage(file, newLeafPageNo, newLeafPage);
                    LeafNodeInt * newLeafNode = (LeafNodeInt *)newLeafPage;
                    // initialize new leaf node
                    int mid = leafOccupancy/2; // middle index
                    for(int i = 0; i < mid; i++) {
                        newLeafNode->keyArray[i] = targetNode->keyArray[mid+i];
                        newLeafNode->ridArray[i] = targetNode->ridArray[mid+i];
                        targetNode->ridArray[mid+i].page_number = 0;
                    }
                    newLeafNode->rightSibPageNo = targetNode->rightSibPageNo;
                    targetNode->rightSibPageNo = newLeafPageNo;

                    if(rootIsLeaf) {
                        // create new root node if leaf node is root node
                        PageId newRootPageNo;
                        Page * newRootPage;
                        bufMgr->allocPage(file, newRootPageNo, newRootPage);
                        NonLeafNodeInt * newRootNode = (NonLeafNodeInt *)newRootPage;

                        // initialize new root node
                        newRootNode->level = 1;
                        newRootNode->keyArray[0] = newLeafNode->keyArray[0];
                        newRootNode->pageNoArray[0] = targetPageNo;
                        newRootNode->pageNoArray[1] = newLeafPageNo;

                        // update meta page
                        Page * metaPage;
                        bufMgr->readPage(file, headerPageNum, metaPage);
                        IndexMetaInfo * metaInfo = (IndexMetaInfo *)metaPage;
                        metaInfo->rootPageNo = newRootPageNo;
                        rootPageNum = newRootPageNo;
                        rootIsLeaf = false;
                        bufMgr->unPinPage(file, newRootPageNo, true);
                        bufMgr->unPinPage(file, headerPageNum, true);
                    } else {
                        // create new PageKeyPair
                        PageKeyPair<int> entry;
                        entry.set(newLeafPageNo, newLeafNode->keyArray[0]);
                        // push up the middle key
                        Page * parentPage;
                        bufMgr->readPage(file, currentPageNo, parentPage);
                        NonLeafNodeInt * parentNode = (NonLeafNodeInt *)parentPage;
                        int pos = nodeOccupancy;
                        // find appropriate position to insert entry
                        while((pos >= 0) && (parentNode->pageNoArray[pos] == 0)) {
                            pos--;
                        }
                        while((pos > 0) && (parentNode->keyArray[pos-1] > entry.key)) {
                            parentNode->keyArray[pos] = parentNode->keyArray[pos-1];
                            parentNode->pageNoArray[pos+1] = parentNode->pageNoArray[pos];
                            pos--;
                        }

                        // insert entry into position
                        parentNode->keyArray[pos] = entry.key;
                        parentNode->pageNoArray[pos+1] = entry.pageNo;
                        bufMgr->unPinPage(file, currentPageNo, true);
                    }

                    bufMgr->unPinPage(file, newLeafPageNo, true);
                    bufMgr->unPinPage(file, targetPageNo, true);

                    // find new target node
                    bufMgr->readPage(file, currentPageNo, currentPage);
                    currentNode = (NonLeafNodeInt *)currentPage;
                    pos = nodeOccupancy;
                    while((pos >= 0) && (currentNode->pageNoArray[pos] == 0)) {
			pos--;
                    }
                    while((pos > 0) && (currentNode->keyArray[pos-1] >= entry.key)) {
			pos--;
                    }

                    currentPageNum = currentNode->pageNoArray[pos];
                    bufMgr->unPinPage(file, currentPageNo, false);

                } else {
                    currentPageNum = targetPageNo;
                    bufMgr->unPinPage(file, targetPageNo, false);
                }
		bufMgr->readPage(file, currentPageNum, currentPageData);
	}

        // find next entry
	currentNode = (LeafNodeInt *)currentPageData;
	for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
		if(lowOpParm == GT) {
			if(currentNode->keyArray[i] > lowValInt) {
				nextEntry = i;
				break;
			} else {
				nextEntry = highValInt+1;
			}

		} else if(lowOpParm == GTE){
			if(currentNode->keyArray[i] >= lowValInt) {
				nextEntry = i;
				break;
			} else {
				nextEntry = highValInt+1;
			}

		} else {
			nextEntry = highValInt+1;
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    if (scanExecuting == false) {
        throw ScanNotInitializedException();
    }
    if (this->currentPageNum == 0) {
        throw IndexScanCompletedException();
    }
    LeafNodeInt * cur = (LeafNodeInt *) this->currentPageData;

    // terminate scan if exceeds limits
    if ((highOp == LT) && (cur->keyArray[nextEntry] >= highValInt)) {
	throw IndexScanCompletedException();
    } else if((highOp == LTE) && (cur->keyArray[nextEntry] > highValInt)) {
	throw IndexScanCompletedException();
    }
    outRid = cur->ridArray[nextEntry];	
    nextEntry++;		
    if( nextEntry == leafOccupancy ||  cur->ridArray[nextEntry].page_number == 0 ){
	this->currentPageNum =  cur->rightSibPageNo;
	if(cur->rightSibPageNo == 0) return;
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
	this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	nextEntry = 0;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    // terminates the current scan
    if (scanExecuting ==  false) {
        // throws ScanNotInitializedException when called before a successful
        // startScan
        throw ScanNotInitializedException();
    } else {
        scanExecuting = false;
    }
    
    //unpin all the pages that have been pinned for the scan
    try {
        bufMgr->unPinPage(this->file, this->currentPageNum, false);
    } catch (PageNotPinnedException e) {}
    
}

}
