#include "heap_storage.h"
#include<queue>
//Yushu Chen, Kaiser Vanguardia

bool test_heap_storage() {return true;}
/* FIXME FIXME FIXME */

typedef u_int16_t u16;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()+4))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id){
    u16 size, loc = get_header(record_id);
    if (loc == 0){
        return;
    }
    Dbt* example = new Dbt(this->address(loc), size);
    return example;
}

void SlottedPage::del(RecordID record_id){
    u16 size, loc = get_header(record_id);
    put_header(record_id,0,0);
    slide(loc, loc + size);

}

void SlottedPage::put(RecordID record_id, const Dbt &data){
    u16 size, loc = get_header(record_id);
    u16 new_size = data->get_size();
    if(new_size > size){
        u16 extra = new_size - size;
        if (!has_room(extra)){
            throw DbBlockNoRoomError("Not enough room in block");
        }
        slide(loc,loc - extra);

        memccpy(this->address(loc -extra), data->get_data(), size);
    } 
    else(){
        memccpy(this->address(loc), data->get_data(), new_size);
        slide(loc+new_size,loc+size);
    }
    u16 size, loc = get_header(RecordID);
    put_header(record_id,new_size,loc);
}

RecordIDs* SlottedPage::ids(void){
    RecordID * a = new RecordIDs();
    for(int i = 1;i<(this->num_records +1);i++){
        if(get_header(i)[1] != 0){
            a.add()
        }
    }
    return a
}

u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

//HeapFile
HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0){
    this->block_sz = BLOCK_SZ;
    this->dbfilename = name;
    this->write_lock = 0;
    std::queue<string> write_queue;
    this->write_queue = write_queue;
}

void HeapFile::open(void){
    db_open();
    this->(this->block_sz) = this->stat['re_len'];
}

void HeapFile::create(void){
    db_open(db->create,db->EXCL);
    this->put(this->get_new());    
}
void HeapFile::close(void){
    this->write_lock = 1;
    this->end_write();
    if(!this->closed){
        //not find the definition, fix later
        close();
        this->closed = True;
    }
}

SlottedPage* HeapFile::get_new(void){
    this->last +=1;
    SlottedPage *b = new SlottedPage(this->block_sz, this->block_id=this->last)
    return b;
}

SlottedPage* HeapFile::get(BlockID block_id){
    int myqueue_size = write_queue.size();
    for(int i = 0; i < myqueue_size; i++) {  
      if(block_id==write_queue[i]){
        return this->write_queue[block_id];
        }
    }
    SlottedPage *SLO = SlottedPage(this->block_sz,this->db->get(block_id),block_id);
   } 

void HeapFile::put(DbBlock *block){
    ++this->write_lock;
    this->write_queue[this->block_id].push(block);
    this->end_write
}
BlockIDs* HeapFile::block_ids(){
    RecordID * a = new RecordIDs();
    for(int i = 1;i<(this->last +1);i++){
            a.add()
    }
}

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes){
	this->file = HeapFile(table_name);
};
Void HeapTable::create(){
	file.create();
}
Void HeapTable::create_if_not_exists(){
	try{
		file.open();
	}
	catch {
file.create();
	}
}
Void HeapTable::open(){
	file.open();
}
Void HeapTable::close(){
	file.close();
}
Void HeapTable::drop(){
	file.delete();
}
Handle HeapTable::insert(const ValueDict *row){
	file.open();
	return file._append(this->file.validate(row));
}
void HeapTable::update(const Handle handle, const ValueDict *new_values){
	
	row = file.project(handle);
	for(key : newValues)
		row[key] = new_values[key];
	full_row = file.validate(row);
	block_id, record_id = handle;
	block = file.get(block_id);
	block.put(record_id, self.marshal(full_row));
	file.put(block);
	return handle;
}
void HeapTable::del(const Handle handle){
	file.open();
	block_id, record_id = handle;
	block = file.get(block_id);
	block.delete(record_id);
	file.put(block);	
}
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}
Dbt* HeapTable::unmarshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
	uint offset = 0;
    uint col_num = 0;
	return data;
}
ValueDict HeapTable::*validate(const ValueDict *row){
//
}
Handle HeapTable::append(const ValueDict *row){
	data = file.marshal(row);
	block = this->file.get(this->file.last);
	try {
	block = file.get_new();	
}
catch {
	block = file.get_new();
	record_id = block.add(data);
}
file.put(block);
return file.last, record;
}