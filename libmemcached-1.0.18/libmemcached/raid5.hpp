/*add a raid5 function*/
#pragma once



//struct check_item *log_list;
struct temp_item
{
  int server_id;//the server item is stored
  char *key;
  char *value;
  struct temp_item *next; 
};

struct HashNode
{
  char *key;
  char *value;
  struct HashNode *next;
};


//temp_item *item_curr,*item_tail;
struct check_item
{
  //int index;//in order
  uint32_t server_id;//fixed server,e.g. the last server
  char *key;
  char *value;
  int index;
  struct  check_item *next;
 /*check_item()
  {
    server_id=0;
    key=(char *)malloc(sizeof(char)*11);
    memcpy(key,0,sizeof(key));
    value=(char *)malloc(sizeof(char)*4);
    memcpy(value,0,sizeof(value));
  }
  check_item(int index,uint32_t server_id,const char *key,const char *value)
  {
     this->index=index;
     this->server_id=server_id;
     memcpy(this->key,key,sizeof(key));
     memcpy(this->value,value,sizeof(value));
  }*/
};

struct t_args
{
    memcached_st *shell;
    const char *key;
    size_t key_length;
    const char *value;
    size_t value_length;
    time_t expiration;
    uint32_t flags;
    uint64_t cas;
    int count;
};

struct HashNode* Init_Node(const char *key,const char *value);
void Hashtable_Init();
uint32_t Get_HashCode(const char *key);
struct HashNode* Hashtable_Find(const char *key,uint32_t hv);
char * Hashtable_GetValue(const char *key);
void Hashtable_Insert_Node(struct HashNode * hn,uint32_t hv);
void Hashtable_Remove(const char *key,uint32_t hv);




void* memcached_send_meta1(void* args);
void* memcached_send_check1(void* args);
char* get_checksum(const char *value1,const char *value2,const char *value3,int length);//count check_sum
int update_checkcum(memcached_st *shell,const char *key,const char *new_value,char *old_value);//update item ,need to update the check_sum
char* get_checksum5(const char *value1,const char *value2,const char *value3,const char *value4,int length);//5 server
check_item* set_checksum(struct check_item *item_list,memcached_st *shell);// 
check_item* add_citem(struct check_item *item_array,uint32_t server_key,const char *key,const char *value);
check_item* del_item(struct check_item *item_list,char *key);
void display(struct check_item *item_array);
memcached_return_t memcached_send_check(memcached_st *shell,
                                                const char *key, size_t key_length,
                                                const char *value, size_t value_length,
                                                const time_t expiration,
                                                const uint32_t flags,
                                                const uint64_t cas,
                                                int count);
memcached_return_t memcached_send_meta(memcached_st *shell,
                                                const char *key, size_t key_length,
                                                const char *value, size_t value_length,
                                                const time_t expiration,
                                                const uint32_t flags,
                                                const uint64_t cas);
memcached_return_t memcached_send_ascii_check(Memcached *ptr,
                                               memcached_instance_st* instance,
                                               const char *key,
                                               const size_t key_length,
                                               const char *value,
                                               const size_t value_length,
                                               const time_t expiration,
                                               const uint32_t flags,
                                               const uint64_t cas,
                                               const bool flush,
                                               const bool reply);
memcached_return_t memcached_send_ascii_check1(Memcached *ptr,
                                               memcached_instance_st* instance,
                                               const char *key,
                                               const size_t key_length,
                                               const char *value,
                                               const size_t value_length,
                                               const time_t expiration,
                                               const uint32_t flags,
                                               const uint64_t cas,
                                               const bool flush,
                                               const bool reply);
