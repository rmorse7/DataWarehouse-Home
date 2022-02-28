
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
    vector<MyDB_PageReaderWriter> pageList;
    discoverPages(rootLocation, pageList, low, high);

    //sorting
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();
    function<bool()> comparator = buildComparator(lhs, rhs);

    MyDB_RecordPtr iterRec = getEmptyRecord();

    MyDB_INRecordPtr lowRec = getINRecord();
    lowRec->setKey(low);
    MyDB_INRecordPtr highRec = getINRecord();
    highRec->setKey(high);

    function <bool()> lowComp = buildComparator(iterRec, lowRec);
    function <bool()> highComp = buildComparator(highRec, iterRec);

	//page iterator to return
    MyDB_RecordIteratorAltPtr pageIter = make_shared<MyDB_PageListIteratorSelfSortingAlt>(pageList, lhs, rhs, comparator, iterRec, lowComp, highComp, true);
    
    return pageIter;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
    vector<MyDB_PageReaderWriter> pageList;
    discoverPages(rootLocation, pageList, low, high);

    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();
    function<bool()> comparator = buildComparator(lhs, rhs);

    MyDB_RecordPtr iterRec = getEmptyRecord();

    MyDB_INRecordPtr lowRec = getINRecord();
    lowRec->setKey(low);
    MyDB_INRecordPtr highRec = getINRecord();
    highRec->setKey(high);

    function <bool()> lowComp = buildComparator(iterRec, lowRec);
    function <bool()> highComp = buildComparator(highRec, iterRec);

	//page iterator to return
    MyDB_RecordIteratorAltPtr pageIter = make_shared<MyDB_PageListIteratorSelfSortingAlt>(pageList, lhs, rhs, comparator, iterRec, lowComp, highComp, false);

    return pageIter;
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &pageList, MyDB_AttValPtr low, MyDB_AttValPtr high) {

    MyDB_PageReaderWriter page = (*this)[whichPage];

    // leaf page
    if (page.getType () == MyDB_PageType :: RegularPage) {

        pageList.push_back (page);
        return true;

    }
    // internal page
    else {

        MyDB_RecordIteratorAltPtr currPtr = page.getIteratorAlt ();

        MyDB_INRecordPtr iterRec = getINRecord ();

        MyDB_INRecordPtr lowRec = getINRecord ();
        lowRec->setKey (low);
        MyDB_INRecordPtr highRec = getINRecord ();
        highRec->setKey (high);

        function <bool ()> lowComp = buildComparator (iterRec, lowRec);
        function <bool ()> highComp = buildComparator (highRec, iterRec);
		
		bool isLeaf = false;
        bool lowInRange = false;
        bool highInRange = true;
        while (currPtr->advance ()) {
            currPtr->getCurrent (iterRec);
            if (!lowComp ()){
                lowInRange = true;
            }


            if(isLeaf){
                if(lowInRange && highInRange){
                    pageList.push_back ((*this)[iterRec->getPtr ()]);
                }
            }
            else{
                if(lowInRange && highInRange){
                    isLeaf = discoverPages (iterRec->getPtr (), pageList, low, high);
                }
            }


            if (highComp ()){
                highInRange = false;
            }

        }
        return false;
    }
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {

    if (getNumPages () <= 1) {

        // root
        MyDB_PageReaderWriter root = (*this)[0];
        rootLocation = 0;

        // internal record
        MyDB_INRecordPtr internalRec = getINRecord ();
        internalRec->setPtr (1);
        getTable ()->setLastPage (1);

        root.clear ();
        root.append (internalRec);
        root.setType (MyDB_PageType :: DirectoryPage);
		
		// leaf
        MyDB_PageReaderWriter leaf = (*this)[1];
        leaf.clear ();
        leaf.setType (MyDB_PageType :: RegularPage);
        leaf.append (appendMe);
        return;
    }
    else {

        auto res = append (rootLocation, appendMe);

        if (res == nullptr) {
        	return;
		}
		
		else {
            lastPage = make_shared<MyDB_PageReaderWriter>(*this, forMe->lastPage()+1);
            lastPage->clear();
            lastPage->append(res);


            MyDB_INRecordPtr anonRec = getINRecord();
            anonRec->setPtr(rootLocation);


            lastPage->append(anonRec);
            lastPage->setType(MyDB_PageType::DirectoryPage);

            forMe->setLastPage (forMe->lastPage () + 1);
            rootLocation = forMe->lastPage();

            forMe->setRootLocation(rootLocation);
        }

    }

}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
	
	// leaf
    if(splitMe.getType() == MyDB_PageType::RegularPage){

        MyDB_RecordPtr tempReclhs = getEmptyRecord();
        MyDB_RecordPtr tempRecrhs = getEmptyRecord();
        function<bool()> comparator = buildComparator(tempReclhs, tempRecrhs);

        splitMe.sortInPlace(comparator, tempReclhs, tempRecrhs);


        vector <MyDB_RecordPtr> tempList;
        MyDB_RecordIteratorAltPtr iterRec = splitMe.getIteratorAlt();
        bool added = false;
        while(iterRec->advance()){
            MyDB_RecordPtr tempRec = getEmptyRecord();
            
            function<bool()> listComp = buildComparator(andMe, tempRec);


            iterRec->getCurrent(tempRec);

            if(!added && listComp()){
                tempList.push_back(andMe);
                added = true;
            }

            tempList.push_back(tempRec);
        }
        if(!added){
            tempList.push_back(andMe);
        }

        lastPage = make_shared<MyDB_PageReaderWriter>(*this, forMe->lastPage()+1);
        lastPage->clear();
        forMe->setLastPage (forMe->lastPage () + 1);
        lastPage->setType(MyDB_PageType::RegularPage);

        int mid = tempList.size() / 2 + tempList.size() % 2;

        for(int i = 0; i < mid; i++){
            lastPage->append(tempList[i]);
        }

        splitMe.clear();
        for(int i = mid; i < tempList.size(); i++){
            splitMe.append(tempList[i]);
        }

        MyDB_INRecordPtr newRec = getINRecord();
        newRec->setPtr(forMe->lastPage ());
        newRec->setKey(getKey(tempList[mid-1]));
        return newRec;
    }
    
	//internal page
    else{
    
        vector <MyDB_RecordPtr> tempList;
        MyDB_RecordIteratorAltPtr iterRec = splitMe.getIteratorAlt();

        bool added = false;
        while(iterRec->advance()){
            MyDB_INRecordPtr tempRec = getINRecord();
            function<bool()> listComp = buildComparator(andMe, tempRec);


            iterRec->getCurrent(tempRec);

            if(!added && listComp()){
                tempList.push_back(andMe);
                added = true;
            }

            tempList.push_back(tempRec);
        }
        if(!added){
            tempList.push_back(andMe);
        }

        lastPage = make_shared<MyDB_PageReaderWriter>(*this, forMe->lastPage() + 1);
        lastPage->clear();
        
        forMe->setLastPage (forMe->lastPage () + 1);
        lastPage->setType(MyDB_PageType::DirectoryPage);


        int mid = tempList.size() / 2 + tempList.size() % 2;


        for(int i = 0; i < mid - 1; i++){
            lastPage->append(tempList[i]);
        }

        splitMe.clear();
        splitMe.setType(MyDB_PageType::DirectoryPage);
        for(int i = mid; i < tempList.size(); i++){
            splitMe.append(tempList[i]);
        }


        MyDB_INRecordPtr newRec = getINRecord();
        MyDB_INRecordPtr midRec = static_pointer_cast<MyDB_INRecord>(tempList[mid - 1]);
        newRec->setPtr(midRec->getPtr());

        lastPage->append(newRec);

        midRec->setPtr(forMe->lastPage ());
		
		//return mid internal record, which is where new page pts
        return midRec;
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	MyDB_PageReaderWriter page = (*this)[whichPage];
	
	// leaf
    if(page.getType() == MyDB_PageType::RegularPage){
        if (page.append(appendMe)){
            return nullptr;
        }
        else {
            return split(page, appendMe);
        }
    }
	
	// internal page
    else{
    	MyDB_INRecordPtr tempRec = getINRecord();
        function<bool()> comparator = buildComparator(tempRec,appendMe);
        
        MyDB_RecordIteratorAltPtr iterRec = page.getIteratorAlt();
        while(iterRec->advance()){
            iterRec->getCurrent(tempRec);

            if(!comparator()){
                break;
            }
        }

        MyDB_RecordPtr internalRec;
        internalRec = append(tempRec->getPtr(), appendMe);


        if(internalRec == nullptr){
            return nullptr;
        }

        else{

            if(page.append(internalRec)){
                MyDB_RecordPtr lhsRec = getINRecord();
                MyDB_RecordPtr rhsRec = getINRecord();
                function<bool()> comparator = buildComparator(lhsRec, rhsRec);
                page.sortInPlace(comparator, lhsRec, rhsRec);
                return nullptr;
            }

            else{
                return split(page,internalRec);
            }
        }
    }
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    queue <MyDB_PageReaderWriter> pagesQ;
    queue <int> pageNumsQ;

    pagesQ.push((*this)[rootLocation]);
    pageNumsQ.push(rootLocation);

    while(!pagesQ.empty()){

        MyDB_PageReaderWriter curPage = pagesQ.front();
        pagesQ.pop();

        int curPageNum = pageNumsQ.front();
        pageNumsQ.pop();

        if(curPage.getType() == MyDB_PageType::DirectoryPage){

            cout << "internal page:" << "page " << curPageNum << ":" << endl;
            MyDB_INRecordPtr tempRec = getINRecord();
            MyDB_RecordIteratorAltPtr curIter = curPage.getIteratorAlt();

            while(curIter->advance()){

                curIter->getCurrent(tempRec);

                cout<<"*"<<(MyDB_RecordPtr)tempRec << endl;

                pagesQ.push((*this)[tempRec->getPtr()]);
                pageNumsQ.push(tempRec->getPtr());
            }
            cout << endl;
        }
        else{

            cout<<"leaf page:" << "page " << curPageNum << ":" << endl;
            MyDB_RecordPtr tempRec = getEmptyRecord();
            MyDB_RecordIteratorAltPtr curIter = curPage.getIteratorAlt();

            while(curIter->advance()){
                curIter->getCurrent(tempRec);
                cout<<"*"<<tempRec<<endl;
            }
            cout << endl;
        }
    }

}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
