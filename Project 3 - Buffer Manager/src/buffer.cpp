/**
 * @author Bill Chang (For CS 564 Fall 2016) (ID: 9074198061)
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */


#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)  // constructor
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() { // destructor
    for (uint32_t i = 0; i < numBufs; i++) {
        if(bufDescTable[i].dirty == true && bufDescTable[i].valid == true) {
            // flush pages to disc if the frame is dirty & valid
            bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
        }
    }
    delete bufPool;
    delete bufDescTable;
    delete hashTable;
}

void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % numBufs; // point to next frame
}

void BufMgr::allocBuf(FrameId & frame) 
{
    // allocates a free frame using the clock algorithm
    uint32_t initialClockHand = clockHand; // keep the initial ClockHand
    uint32_t pinnedCounter = 0; // buffer frame pinned count
    
    advanceClock(); // point to next frame
    while (pinnedCounter < numBufs) {
        if (clockHand == initialClockHand) {
            // reset counter when point back to the initial ClockHand
            pinnedCounter = 0;
        }
        if (bufDescTable[clockHand].valid == false) {
            // valid set? Set on the frame if it's not used
            frame = bufDescTable[clockHand].frameNo;
            break;
        } else if (bufDescTable[clockHand].refbit == true) {
            // refbit set? clear and point to next frame
            bufDescTable[clockHand].refbit = false;
            advanceClock();
        } else if (bufDescTable[clockHand].pinCnt > 0) {
            // page pinned, increase count and point to next frame
            advanceClock();
            pinnedCounter ++;
        } else {
            if (bufDescTable[clockHand].dirty == true) {
                // the selected buffer frame is dirty, the page currently occupying the frame is written back to disk
                bufDescTable[clockHand].file->writePage(bufPool[bufDescTable[clockHand].frameNo]);
            }
            // remove and clear the frame from hash table and buffer table
            hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
            bufDescTable[clockHand].Clear();
            // return the frame number which is free now to allocate
            frame = bufDescTable[clockHand].frameNo;
            break;
        }
    }
    if (pinnedCounter == numBufs) {
        // throw BEE if all buffer frames are pinned
        throw BufferExceededException();
    }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try  {
		// check if (file.pageNo) is in bufPool. This is case 2.
		hashTable->lookup(file, pageNo, frameNo);
		// set appropriate reference bit
		bufDescTable[frameNo].refbit = true;
		// increment pinCnt
		bufDescTable[frameNo].pinCnt ++;
		page = &bufPool[frameNo]; // return the page by ref
	} catch (HashNotFoundException e) {
		// lookup method will throw HNF exception if page is not in the buffer
		// pool. This is Case 1.
		allocBuf(frameNo); // allocate and read from disk into bufPool frame
		// add the page to buffer pool
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo); // insert into hashtable
		// invoke Set() on the frame to set it up properly
		// Set() will leave the pinCnt to be 1
		bufDescTable[frameNo].Set(file, pageNo);
		page = &bufPool[frameNo]; // return page by ref
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo); // if found, continue
		if(bufDescTable[frameNo].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frameNo); // throw PNP exception if pinCnt is 0
		}
		if (dirty == true) {
			bufDescTable[frameNo].dirty = true;	// set dirty if (dirty == true)
		}
		bufDescTable[frameNo].pinCnt --; // decrement pinCnt
	} catch (HashNotFoundException e) {
		// do nothing otherwise
	}
}

void BufMgr::flushFile(const File* file) 
{
	for (uint32_t i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) {
			if (bufDescTable[i].pinCnt > 0) {
				// throw PP exception if some page of the file is pinned
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			if (bufDescTable[i].valid == false) {
				// throw BB exception if an invalid page belonging to the file is encountered
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			if (bufDescTable[i].dirty == true) {
				// (a) call file ->writePage() tp flush the page to disk if it's dirty
				bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
				// then set the dirty bit for the page to false
				bufDescTable[i].dirty = false;
			}
			// (b) remove the page from the hashtable
			hashTable->remove(file, bufDescTable[i].pageNo);
			// (c) invoke the Clear() method of BufDesc for the page frame
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNo;
	allocBuf(frameNo); //obtain a buffer pool frame
	// allocate an empty page and put it at the buffer pool frame
	bufPool[frameNo] = file->allocatePage();
	// an entry is inserted into the hash table
	hashTable->insert(file, bufPool[frameNo].page_number(), frameNo);
	// Set() is invoked on the frame
	bufDescTable[frameNo].Set(file, bufPool[frameNo].page_number());
	// return the page number of the newly allocated page to the caller via
	// the pageNo parameter
	pageNo = bufPool[frameNo].page_number();
	// return a pointer to the buffer frame allocated for the page via page
	// parameter
	page = &bufPool[frameNo];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameNo;
    // check the page to be deleted is allocated in a frame in the buffer pool
    hashTable->lookup(file, PageNo, frameNo);
    bufDescTable[frameNo].Clear(); // free the frame
    // correspondingly entry from hash table is also removed
    hashTable->remove(file, PageNo);
    file->deletePage(PageNo); // delete the page
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
