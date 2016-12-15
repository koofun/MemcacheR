#include "libmemcached/common.h"
#include "../libhashkit/common.h"
#include <pthread.h>
extern uint32_t count_check0,count_check1,count_check2,count_check3;
#define  COUNT  4
#define HASH_MAX_SIZE 1000003
struct HashNode* hashtable[HASH_MAX_SIZE];// 素数

char* get_checksum(const char *value1,const char *value2,const char *value3,int length)
{
  if(value1==NULL||value2==NULL||value3==NULL||length<=0)  return NULL;
  char *result;
  result=(char*)malloc(length*sizeof(char)+1);
  int i=0;
  for(i=0;i<length;i++)
  {
     if(value1[i]=='@')//restore key
     {
        result[i]=(127)^value2[i]^value3[i];
     
     }
    else if(value2[i]=='@')//restore key
     {
        result[i]=(127)^value1[i]^value3[i];
     
     }
     else if(value3[i]=='@')//restore key
     {
        result[i]=(127)^value2[i]^value1[i];
     
     }
     else // just compute p
     {
         result[i]=value1[i]^value2[i]^value3[i];
          if(result[i]==127) result[i]='@'; //because 127 in ascii talbe can not print,so we use @ to replace it.
     }
  }
  result[i]='\0';
  return result; 
}

check_item* set_checksum(struct check_item *item_list,memcached_st *shell)
{
  if(item_list==NULL) {return NULL;}
  memcached_return_t error;
  uint32_t flags;
  size_t val_len;
  uint32_t hv=0;
  check_item *head=item_list;
  int n_key=strlen(item_list->key);
  int n_value=strlen(item_list->value);
  char key[COUNT][n_key+1];
  char value[COUNT][n_value+1];
  char tmp_key[COUNT-1][n_key+1];//sms key 
  char tmp_value[COUNT-1][n_value+1];//sms value 
  int flag[COUNT+1];
  for(int i=0;i<COUNT;i++) flag[i]=0;
  static int server=0;
  static int check_num=0;
  char keys[3*n_key+2];//3 keys
  while(item_list)
  {
     if(item_list->server_id==0&&flag[0]==0)//get the first item in server0
     {
       strncpy(key[0],item_list->key,n_key);
        //strcpy(key0,item_list->key);
        key[0][n_key]='\0';
        strncpy(value[0],item_list->value,n_value);
        value[0][n_value]='\0';
        flag[0]=1;
     }
     else if(item_list->server_id==1&&flag[1]==0)//get the first item in server1
     {
        strncpy(key[1],item_list->key,n_key);
        key[1][n_key]='\0';
        strncpy(value[1],item_list->value,n_value);
        value[1][n_value]='\0';
        flag[1]=1;
     }
     else if(item_list->server_id==2&&flag[2]==0)//get the first item in server2
     {
        strncpy(key[2],item_list->key,n_key);
        key[2][n_key]='\0';
        strncpy(value[2],item_list->value,n_value);
        value[2][n_value]='\0';
        flag[2]=1;
     }
     else if(item_list->server_id==3&&flag[3]==0)//get the first item in server3
     {
        strncpy(key[3],item_list->key,n_key);
        key[3][n_key]='\0';
        strncpy(value[3],item_list->value,n_value);
        value[3][n_value]='\0';
        flag[3]=1;
     }
//printf("flag0=%d,flag1=%d,flag2=%d,flag3=%d\n",flag0,flag1,flag2,flag3);
     if(flag[0]==1&&flag[1]==1&&flag[2]==1&&server==0)// checksum in server3
     {
          strncpy(tmp_key[0],key[0],n_key+1);strncpy(tmp_value[0],value[0],n_value+1);//tmp_keyA[10]='\0';
          strncpy(tmp_key[1],key[1],n_key+1);strncpy(tmp_value[1],value[1],n_value+1);//tmp_keyB[10]='\0';
          strncpy(tmp_key[2],key[2],n_key+1);strncpy(tmp_value[2],value[2],n_value+1);//tmp_keyC[10]='\0';
          flag[COUNT]=1;server=3;
          break;
      }
      else if(flag[0]==1&&flag[1]==1&&flag[3]==1&&server==3)// checksum in server2
     {
          strncpy(tmp_key[0],key[0],n_key+1);strncpy(tmp_value[0],value[0],n_value+1);//tmp_keyA[10]='\0';
          strncpy(tmp_key[1],key[1],n_key+1);strncpy(tmp_value[1],value[1],n_value+1);//tmp_keyB[10]='\0';
          strncpy(tmp_key[2],key[3],n_key+1);strncpy(tmp_value[2],value[3],n_value+1);//tmp_keyC[10]='\0';
	  flag[COUNT]=1;server=2;
          break;
      }
      else if(flag[0]==1&&flag[2]==1&&flag[3]==1&&server==2)// checksum in server1
     {
	  strncpy(tmp_key[0],key[0],n_key+1);strncpy(tmp_value[0],value[0],n_value+1);//tmp_keyA[10]='\0';
          strncpy(tmp_key[1],key[2],n_key+1);strncpy(tmp_value[1],value[2],n_value+1);//tmp_keyB[10]='\0';
          strncpy(tmp_key[2],key[3],n_key+1);strncpy(tmp_value[2],value[3],n_value+1);//tmp_keyC[10]='\0';
          flag[COUNT]=1;server=1;
          break;
      }
     else if(flag[1]==1&&flag[2]==1&&flag[3]==1&&server==1)// checksum in server0
     {
	  strncpy(tmp_key[0],key[1],n_key+1);strncpy(tmp_value[0],value[1],n_value+1);//tmp_keyA[10]='\0';
          strncpy(tmp_key[1],key[2],n_key+1);strncpy(tmp_value[1],value[2],n_value+1);//tmp_keyB[10]='\0';
          strncpy(tmp_key[2],key[3],n_key+1);strncpy(tmp_value[2],value[3],n_value+1);//tmp_keyC[10]='\0';
          flag[COUNT]=1;server=0;
          break;
      }
     item_list=item_list->next;
  }
  item_list=head;//point to list head
  if(flag[COUNT]==1)//get the all the stripe info,count checksum and store to server
  {
   //printf("tmp_key[0]=%s,tmp_key[1]=%s,tmp_key[2]=%s\n",tmp_key[0],tmp_key[1],tmp_key[2]);
    char *check_key,*check_value,*str1,*str2,*str3;
    char temp_key[n_key+2];
    char temp_key_t[n_key+2];
    check_key=get_checksum(tmp_key[0],tmp_key[1],tmp_key[2],n_key);//get check_key
    check_key[n_key]='\0';
    check_value=get_checksum(tmp_value[0],tmp_value[1],tmp_value[2],n_value);//get check_value
    check_value[n_value]='\0';
    int count=server;
    memcached_send_check(shell,check_key,n_key,check_value,n_value,3600,0,0,count);//store checksum   
    
    /*//set sms info
    strncpy(temp_key,tmp_key[1],n_key);temp_key[n_key]=count+'0';temp_key[n_key+1]=0;
    memcached_send_meta(shell,tmp_key[0],n_key,temp_key,n_key+1,3600,0,0);//store stripe info and p_server_id
    strncpy(temp_key,tmp_key[2],n_key);temp_key[n_key]=count+'0';temp_key[n_key+1]=0;
    memcached_send_meta(shell,tmp_key[1],n_key,temp_key,n_key+1,3600,0,0);//store stripe info and p_server_id
    strncpy(temp_key,tmp_key[0],n_key);temp_key[n_key]=count+'0';temp_key[n_key+1]=0;
    memcached_send_meta(shell,tmp_key[2],n_key,temp_key,n_key+1,3600,0,0);//store stripe info and p_server_id
    */

    //store sms to hash table
    struct HashNode* hn=NULL;
    strncpy(temp_key,tmp_key[1],n_key);temp_key[n_key]=count+'0';temp_key[n_key+1]=0;
    hn=Init_Node(tmp_key[0],temp_key);
    hv=hashkit_jenkins(tmp_key[0],n_key,0);//get hashkey 
    Hashtable_Insert_Node(hn,hv);//insert node to hashtable
    
    strncpy(temp_key,tmp_key[2],n_key);temp_key[n_key]=count+'0';temp_key[n_key+1]=0;
    hn=Init_Node(tmp_key[1],temp_key);
    hv=hashkit_jenkins(tmp_key[1],n_key,0);//get hashkey 
    Hashtable_Insert_Node(hn,hv);//insert node to hashtable
 
    strncpy(temp_key,tmp_key[0],n_key);temp_key[n_key]=count+'0';temp_key[n_key+1]=0;
    hn=Init_Node(tmp_key[2],temp_key);
    hv=hashkit_jenkins(tmp_key[2],n_key,0);//get hashkey 
    Hashtable_Insert_Node(hn,hv);//insert node to hashtable
    
    //get all the keys,then send to backup
    for(int i=0;i<3;i++)
    {
        for(int j=0;j<n_key;j++)
       {
          keys[i*n_key+j]=tmp_key[i][j];
       }
    }
    keys[3*n_key]='0'+count;
    keys[3*n_key+1]='\0';
    printf("----------keys=%s\n---",keys);
    send_keys(keys);//send keys to other client ,insert to hashtable
//printf("----------------------------------\n");
    item_list=del_item(item_list,tmp_key[0]);
    item_list=del_item(item_list,tmp_key[1]);
    item_list=del_item(item_list,tmp_key[2]);
  }
  /*else//get the part of the stripe info
  {
     printf("cannot find a stripe\n");//Todo
  }*/
  return item_list;
}

check_item* add_citem(struct check_item *item_array,uint32_t server_key,const char *key,const char *value)
{
  check_item *c_item,*head;
  int n_key,n_value;
  n_key=strlen(key);
  n_value=strlen(value);
  c_item=NULL;

  head=item_array;
  c_item=(struct check_item *)malloc(sizeof(struct check_item));
  c_item->key=(char*)malloc(sizeof(char)*n_key+1);
  c_item->value=(char*)malloc(sizeof(char)*n_value+1);

  strncpy(c_item->key,key,n_key);//
  c_item->key[n_key]='\0';
  strncpy(c_item->value,value,n_value);//memcpy(c_item->value,value,sizeof(value))
  c_item->value[n_value]='\0';

  c_item->server_id=server_key; 
  c_item->next=NULL;

  if(item_array==NULL)
  {
     item_array=c_item;
  }
  else
  {
    while(head->next)
    {
       head=head->next;
    }
    head->next=c_item;
  }
  return item_array;
}

check_item* del_item(struct check_item *item_list,char *key)
{
    //printf("in del_item\n");
    check_item *head,*prev,*del;
    head=item_list;prev=item_list;
    while(head)
   {
      if(strcmp(head->key,key)==0)
        break;
      prev=head;
      head=head->next;
   }
   if(head==NULL)  { printf("cannot find the item\n");return item_list;}
   if(head==item_list) 
   {//printf("del first item key=%s\n",item_list->key);
     item_list=prev->next; 
    }//
   else 
   { //printf("del  item key=%s\n",head->key);
     del=head;
     prev->next=del->next;
     del=NULL;
    }
    return item_list;
}

int update_checkcum(memcached_st *shell,const char *key,const char *new_value,char *old_value)
{
  if(key==NULL) return -1;
  char *key1,*key2,*key_p,*value1,*value2,*value_p;
  key1=key2=key_p=NULL;value1=value2=value_p=NULL;
  int server_p;//the id of check_sum
  size_t key_length=strlen(key);
  int len=strlen(new_value);//the same as old value
  size_t *v_length=NULL;
  uint32_t flags;
  size_t *value_length=NULL;
  memcached_return_t *error=NULL;
//printf("key=%s,old_value=%s\n",key,old_value);
//printf("key=%s,new_value=%s\n",key,new_value);
  if((key1=memcached_degrade_get_by_key(shell, NULL, 0, key, key_length, v_length,&flags, error,-1))!=NULL)//get key1
  {

//value1=memcached_get_by_key(shell, NULL, 0, key1, key_length, value_length,&flags, error);
//printf("key1=%s,value1=%s\n",key1,value1);
     if((key2=memcached_degrade_get_by_key(shell, NULL, 0, key1, key_length, v_length,&flags, error,-1))!=NULL)//get key2
     {
//value2=memcached_get_by_key(shell, NULL, 0, key2, key_length, value_length,&flags, error);
//printf("key2=%s,value2=%s\n",key2,value2);
	key_p=get_checksum(key,key1,key2,key_length);//get check_key
        server_p=key1[key_length]-'0';
        if((value_p=memcached_degrade_get_by_key(shell, NULL, 0, key_p, key_length, v_length,&flags, error,server_p))!=NULL)// get value_p
        {
	  //printf("value_p=%s\n",value_p);
	  char *new_value_p=get_checksum(new_value,old_value,value_p,len);
	  //printf("value_p=%s\n",new_value_p);
	  memcached_send_check(shell,key_p,key_length,new_value_p,len,3600,0,0,server_p);//store checksum
	}
      }
  }
  if(key1==NULL||key2==NULL||value_p==NULL)
  {
     //printf("update check_sum failed!\n");
     return -1;
  }
  return 0;
}

void display(struct check_item *item_array)
{
  check_item *head=item_array;
  while(head)
  {
    
    printf("head->server_id=%d,head->key=%s,head->value=%s\n",head->server_id,head->key,head->value);
    head=head->next;
  }
}
/*
struct t_args
{
    memcached_st *shell;
    const char *key;
    size_t key_length;
    const char *value;
    size_t value_length;
    const time_t expiration;
    const uint32_t flags;
    const uint64_t cas;
    int count;
};
*/
void* memcached_send_check1(void * args)
{
if(args==NULL) {printf("args id NULL\n");return NULL;}
//printf("in memcached_send_check1\n");  
   struct t_args *info=NULL;
	 info=(struct t_args*)args ;
	 Memcached* ptr= memcached2Memcached(info->shell);
  memcached_return_t rc;
  if (memcached_failed(rc= initialize_query(ptr, true)))
  {
    return &rc;
  }
  if (memcached_failed(memcached_key_test(*ptr, (const char **)&(info->key), &(info->key_length), 1)))
  { 

     rc=(memcached_last_error(ptr)); return &rc;
  }
  uint32_t server_key_check=info->count;//
  memcached_instance_st* instance_check= memcached_instance_fetch(ptr, server_key_check);

  WATCHPOINT_SET(instance_check->io_wait_count.read= 0);
  WATCHPOINT_SET(instance_check->io_wait_count.write= 0);

  bool flush= true;
  if (memcached_is_buffering(instance_check->root))
  {
printf("5555555555555flush is false5555555555555\n");
    flush= false;
  }

  bool reply= memcached_is_replying(ptr);

  hashkit_string_st* destination= NULL;

  if (memcached_is_encrypted(ptr))
  {
    
    if ((destination= hashkit_encrypt(&ptr->hashkit, info->value, info->value_length)) == NULL)
    {
      return &rc;
    }
    info->value= hashkit_string_c_str(destination);
    info->value_length= hashkit_string_length(destination);
  }
    rc= memcached_send_ascii_check1(ptr, instance_check,
                             info->key, info->key_length,
                             info->value, info->value_length, info->expiration,
                             info->flags, info->cas, flush, reply);
    
  hashkit_string_free(destination);
  return &rc;
}
void* memcached_send_meta1(void* args)
{
  struct t_args *info=NULL;
  info=(struct t_args*)args ;
  Memcached* ptr= memcached2Memcached(info->shell);
  memcached_return_t rc;
  if (memcached_failed(rc= initialize_query(ptr, true)))
  {
    return &rc;
  }
  if (memcached_failed(memcached_key_test(*ptr, (const char **)&(info->key), &(info->key_length), 1)))
  {
    rc=memcached_last_error(ptr); return &rc;
  }
  //store the sms info in the next server after the key
  uint32_t server_key= memcached_generate_hash_with_redistribution(ptr, info->key, info->key_length);
  uint32_t server_key_meta=(server_key+1)%memcached_server_count(ptr);
  memcached_instance_st* instance_meta= memcached_instance_fetch(ptr, server_key_meta);

  WATCHPOINT_SET(instance_meta->io_wait_count.read= 0);
  WATCHPOINT_SET(instance_meta->io_wait_count.write= 0);

  bool flush= true;
  if (memcached_is_buffering(instance_meta->root))
  {
    flush= false;
  }

  bool reply= memcached_is_replying(ptr);

  hashkit_string_st* destination= NULL;

  if (memcached_is_encrypted(ptr))
  {
    
    if ((destination= hashkit_encrypt(&ptr->hashkit, info->value, info->value_length)) == NULL)
    {
      return &rc;
    }
    info->value= hashkit_string_c_str(destination);
    info->value_length= hashkit_string_length(destination);
  }
  rc= memcached_send_ascii_check(ptr, instance_meta,
                             info->key, info->key_length,
                             info->value, info->value_length, info->expiration,
                             info->flags, info->cas, flush, reply);
  hashkit_string_free(destination);
  return &rc;
}
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
                                               const bool reply)
{
  char flags_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int flags_buffer_length= snprintf(flags_buffer, sizeof(flags_buffer), " %u", flags);
  if (size_t(flags_buffer_length) >= sizeof(flags_buffer) or flags_buffer_length < 0)
  {
    return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                               memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
  }

  char expiration_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int expiration_buffer_length= snprintf(expiration_buffer, sizeof(expiration_buffer), " %llu", (unsigned long long)expiration);
  if (size_t(expiration_buffer_length) >= sizeof(expiration_buffer) or expiration_buffer_length < 0)
  {
    return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                               memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
  }

  char value_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int value_buffer_length= snprintf(value_buffer, sizeof(value_buffer), " %llu", (unsigned long long)value_length);
  if (size_t(value_buffer_length) >= sizeof(value_buffer) or value_buffer_length < 0)
  {
    return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                               memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
  }

  char cas_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int cas_buffer_length= 0;
  if (cas)
  {
    cas_buffer_length= snprintf(cas_buffer, sizeof(cas_buffer), " %llu", (unsigned long long)cas);
    if (size_t(cas_buffer_length) >= sizeof(cas_buffer) or cas_buffer_length < 0)
    {
      return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                                 memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
    }
  }

  libmemcached_io_vector_st vector[]=
  {
    { NULL, 0 },
    { "set ", strlen("set ")},
    { memcached_array_string(ptr->_namespace), memcached_array_size(ptr->_namespace) },
    { key, key_length },
    { flags_buffer, size_t(flags_buffer_length) },
    { expiration_buffer, size_t(expiration_buffer_length) },
    { value_buffer, size_t(value_buffer_length) },
    //{ index_buffer,size_t(index_buffer_length)},//add index  wf
    { cas_buffer, size_t(cas_buffer_length) },
    { " noreply", reply ? 0 : memcached_literal_param_size(" noreply") },
    { memcached_literal_param("\r\n") },
    { value, value_length },
    { memcached_literal_param("\r\n") }
  };

  /* Send command header */
  memcached_return_t rc=  memcached_vdo(instance, vector, 12, flush);

  // If we should not reply, return with MEMCACHED_SUCCESS, unless error
  if (reply == false)
  {
    return memcached_success(rc) ? MEMCACHED_SUCCESS : rc; 
  }

  if (flush == false)
  {
    return memcached_success(rc) ? MEMCACHED_BUFFERED : rc; 
  }
//printf("rc1=====%d\n",rc);
  if (rc == MEMCACHED_SUCCESS)
  {
    char buffer[MEMCACHED_DEFAULT_COMMAND_SIZE];
    rc= memcached_response(instance, buffer, sizeof(buffer), NULL);

    if (rc == MEMCACHED_STORED)
    {
//printf("rc2=====%d\n",rc);
      return MEMCACHED_SUCCESS;
    }
  }
//printf("rc3=====%d\n",rc);
  if (rc == MEMCACHED_WRITE_FAILURE)
  {
printf("reset\n");
    memcached_io_reset(instance);
  }

  assert(memcached_failed(rc));
printf("BBBBBBBBBBrc=%d\n",rc);
#if 0
  if (memcached_has_error(ptr) == false)
  {
    return memcached_set_error(*ptr, rc, MEMCACHED_AT);
  }
#endif

  return rc;
}

memcached_return_t memcached_send_check(memcached_st *shell,
                                                const char *key, size_t key_length,
                                                const char *value, size_t value_length,
                                                const time_t expiration,
                                                const uint32_t flags,
                                                const uint64_t cas,
                                                int count)
{
  Memcached* ptr= memcached2Memcached(shell);
  memcached_return_t rc;
  if (memcached_failed(rc= initialize_query(ptr, true)))
  {
    return rc;
  }
  if (memcached_failed(memcached_key_test(*ptr, (const char **)&key, &key_length, 1)))
  {
    return memcached_last_error(ptr);
  }
  uint32_t server_key_check=count;//
  memcached_instance_st* instance_check= memcached_instance_fetch(ptr, server_key_check);

  WATCHPOINT_SET(instance_check->io_wait_count.read= 0);
  WATCHPOINT_SET(instance_check->io_wait_count.write= 0);

  bool flush= true;
  if (memcached_is_buffering(instance_check->root))
  {
    flush= false;
  }

  bool reply= memcached_is_replying(ptr);

  hashkit_string_st* destination= NULL;

  if (memcached_is_encrypted(ptr))
  {
    
    if ((destination= hashkit_encrypt(&ptr->hashkit, value, value_length)) == NULL)
    {
      return rc;
    }
    value= hashkit_string_c_str(destination);
    value_length= hashkit_string_length(destination);
  }
    rc= memcached_send_ascii_check(ptr, instance_check,
                             key, key_length,
                             value, value_length, expiration,
                             flags, cas, flush, reply);
    
  hashkit_string_free(destination);
  return rc;
}

memcached_return_t memcached_send_meta(memcached_st *shell,
                                                const char *key, size_t key_length,
                                                const char *value, size_t value_length,
                                                const time_t expiration,
                                                const uint32_t flags,
                                                const uint64_t cas
                                                )
{
  Memcached* ptr= memcached2Memcached(shell);
  memcached_return_t rc;
  if (memcached_failed(rc= initialize_query(ptr, true)))
  {
    return rc;
  }
  if (memcached_failed(memcached_key_test(*ptr, (const char **)&key, &key_length, 1)))
  {
    return memcached_last_error(ptr);
  }
  //store the sms info in the next server after the key
  uint32_t server_key= memcached_generate_hash_with_redistribution(ptr, key, key_length);
  uint32_t server_key_meta=(server_key+1)%memcached_server_count(ptr);
  memcached_instance_st* instance_meta= memcached_instance_fetch(ptr, server_key_meta);

  WATCHPOINT_SET(instance_meta->io_wait_count.read= 0);
  WATCHPOINT_SET(instance_meta->io_wait_count.write= 0);

  bool flush= true;
  if (memcached_is_buffering(instance_meta->root))
  {
printf("5555555555555flush is false5555555555555\n");
    flush= false;
  }

  bool reply= memcached_is_replying(ptr);

  hashkit_string_st* destination= NULL;

  if (memcached_is_encrypted(ptr))
  {
    
    if ((destination= hashkit_encrypt(&ptr->hashkit, value, value_length)) == NULL)
    {
      return rc;
    }
    value= hashkit_string_c_str(destination);
    value_length= hashkit_string_length(destination);
  }
  /*if(server_key_meta==0) count_check0++;
    else if(server_key_meta==1) count_check1++;
    else if(server_key_meta==2) count_check2++;
    else if(server_key_meta==3) count_check3++;*/
  if (memcached_is_binary(ptr))
  {
    /*rc= memcached_send_binary(ptr, instance_check, server_key_meta,
                              key, key_length,
                              value, value_length, expiration,
                              flags, cas, flush, reply, 0);*/
   //we don't support
  }
  else
  {
    rc= memcached_send_ascii_check(ptr, instance_meta,
                             key, key_length,
                             value, value_length, expiration,
                             flags, cas, flush, reply);
    
   }

  hashkit_string_free(destination);

  return rc;
}

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
                                               const bool reply)
{
  char flags_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int flags_buffer_length= snprintf(flags_buffer, sizeof(flags_buffer), " %u", flags);
  if (size_t(flags_buffer_length) >= sizeof(flags_buffer) or flags_buffer_length < 0)
  {
    return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                               memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
  }

  char expiration_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int expiration_buffer_length= snprintf(expiration_buffer, sizeof(expiration_buffer), " %llu", (unsigned long long)expiration);
  if (size_t(expiration_buffer_length) >= sizeof(expiration_buffer) or expiration_buffer_length < 0)
  {
    return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                               memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
  }

  char value_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int value_buffer_length= snprintf(value_buffer, sizeof(value_buffer), " %llu", (unsigned long long)value_length);
  if (size_t(value_buffer_length) >= sizeof(value_buffer) or value_buffer_length < 0)
  {
    return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                               memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
  }
  
   char index_buffer[8+1];
  int index_buffer_length;
  //send index to target server
  /*if(server_key==0)
  {
     index_buffer_length=snprintf(index_buffer,sizeof(index_buffer)," %llu",count_check0);
  }
  else if(server_key==1)
  {
     index_buffer_length=snprintf(index_buffer,sizeof(index_buffer)," %llu",count_check1);
  }
  else if(server_key==2)
  {
     index_buffer_length=snprintf(index_buffer,sizeof(index_buffer)," %llu",count_check2);
  }
  else if(server_key==3)
  {
     index_buffer_length=snprintf(index_buffer,sizeof(index_buffer)," %llu",count_check3);
  }*/

  
  char cas_buffer[MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH +1];
  int cas_buffer_length= 0;
  if (cas)
  {
    cas_buffer_length= snprintf(cas_buffer, sizeof(cas_buffer), " %llu", (unsigned long long)cas);
    if (size_t(cas_buffer_length) >= sizeof(cas_buffer) or cas_buffer_length < 0)
    {
      return memcached_set_error(*instance, MEMCACHED_MEMORY_ALLOCATION_FAILURE, MEMCACHED_AT, 
                                 memcached_literal_param("snprintf(MEMCACHED_MAXIMUM_INTEGER_DISPLAY_LENGTH)"));
    }
  }

  libmemcached_io_vector_st vector[]=
  {
    { NULL, 0 },
    { "set ", strlen("set ")},
    { memcached_array_string(ptr->_namespace), memcached_array_size(ptr->_namespace) },
    { key, key_length },
    { flags_buffer, size_t(flags_buffer_length) },
    { expiration_buffer, size_t(expiration_buffer_length) },
    { value_buffer, size_t(value_buffer_length) },
    //{ index_buffer,size_t(index_buffer_length)},//add index  wf
    { cas_buffer, size_t(cas_buffer_length) },
    { " noreply", reply ? 0 : memcached_literal_param_size(" noreply") },
    { memcached_literal_param("\r\n") },
    { value, value_length },
    { memcached_literal_param("\r\n") }
  };

  /* Send command header */
  memcached_return_t rc=  memcached_vdo(instance, vector, 12, flush);

  // If we should not reply, return with MEMCACHED_SUCCESS, unless error
  if (reply == false)
  {
    return memcached_success(rc) ? MEMCACHED_SUCCESS : rc; 
  }

  if (flush == false)
  {
    return memcached_success(rc) ? MEMCACHED_BUFFERED : rc; 
  }

  if (rc == MEMCACHED_SUCCESS)
  {
    char buffer[MEMCACHED_DEFAULT_COMMAND_SIZE];
    rc= memcached_response(instance, buffer, sizeof(buffer), NULL);

    if (rc == MEMCACHED_STORED)
    {
      return MEMCACHED_SUCCESS;
    }
  }

  if (rc == MEMCACHED_WRITE_FAILURE)
  {
printf("reset\n");
    memcached_io_reset(instance);
  }

  assert(memcached_failed(rc));

//printf("AAAAAAAAAAArc=%d\n",rc);
#if 0
  if (memcached_has_error(ptr) == false)
  {
    return memcached_set_error(*ptr, rc, MEMCACHED_AT);
  }
#endif

  return rc;
}

struct HashNode* Init_Node(const char *key,const char *value)
{
	struct HashNode *hn=NULL;
	hn=(struct HashNode*)malloc(sizeof(struct HashNode));
	hn->key=(char *)malloc(sizeof(char)*strlen(key)+1);
	strcpy(hn->key,key);
	hn->value =(char *)malloc(sizeof(char)*strlen(value)+1);
	strcpy(hn->value,value);
	hn->next =NULL;
	return hn;
}

void Hashtable_Init()
{
  printf("init hashtable...\n");
  memset(hashtable,0,sizeof(HashNode*)*HASH_MAX_SIZE);
  pthread_t thd;
  pthread_create(&thd,NULL,recv_keys,(void*)NULL);
}

uint32_t Get_HashCode(const char *key)
{
    uint32_t hv=0;
    uint32_t x=0;
    while(*key)
    {
        hv=(hv<<4)+(*key++);//hv左移4位，当前字符ASCII存入h的低四位
        if( (x=hv & 0xF0000000L)!=0) //如果最高位不为0，则说明字符多余7个，如果不处理，再加第九个字符时，第一个字符会被移出,因此要有如下处理
        {
          hv^=(x>>24);
          hv&=~x;//清空28~31位 
        }
	}
	return hv;
}

struct HashNode* Hashtable_Find(const char *key,uint32_t hv)
{
	if(key==NULL) return NULL;
	struct HashNode *hn=NULL;
	//uint32_t hv=Get_HashCode(key);
	hn=hashtable[hv%HASH_MAX_SIZE];
	struct HashNode *ret=NULL;
	while(hn)
	{
		if(strcmp(key,hn->key)==0) 
		{
			ret=hn;
			break;
		}
		hn=hn->next;
	}
	return ret;
}

char * Hashtable_GetValue(const char *key)
{
    if(key==NULL) return NULL;
    char *ret_value=NULL;

    uint32_t hv=hashkit_jenkins(key,strlen(key),0);//get hashkey 
    struct HashNode* hn=hashtable[hv%HASH_MAX_SIZE];
    while(hn)
    {
        if(strcmp(key,hn->key)==0)
        {    
            ret_value=hn->value;
            break;
        }
        hn=hn->next;
    }
    if(hn==NULL) printf("cannot find\n");
    return ret_value;
}

void Hashtable_Insert_Node(struct HashNode * hn,uint32_t hv)
{
        //printf("insert to hash table key=%s\n",hn->key);
	if(hn==NULL) return ;
	hn->next=hashtable[hv%HASH_MAX_SIZE];//头插
	hashtable[hv%HASH_MAX_SIZE]=hn;
}


void Hashtable_Remove(const char *key,uint32_t hv)
{
	if(key==NULL) return;
	struct HashNode *ht=NULL;
	//uint32_t hv=Get_HashCode(key);
	ht=hashtable[hv%HASH_MAX_SIZE];
	if(ht&&(strcmp(key,ht->key)==0)) //第一个就是
	{
		hashtable[hv%HASH_MAX_SIZE]=ht->next;
	}
	else
	{
		struct HashNode * pre=ht;
		while(ht)
		{
			
			if(strcmp(key,ht->key)==0) break;
			pre=ht;
			ht=ht->next;
		}
		pre->next=ht->next;
		ht=NULL;
	}
}

void store_keys(const char *keys,int n_keys)
{
   int len=(n_keys-1)/3;
   char key1[len+1];
   char key2[len+2];
   printf("keys=%s\n",keys);
     
   
   strncpy(key1,keys,len);key1[len]='\0';
   strncpy(key2,keys+len,len);
   key2[len]=keys[n_keys-1];
   key2[len+1]='\0';
    struct HashNode* hn=NULL;
    hn=Init_Node(key1,key2);
    uint32_t hv=hashkit_jenkins(key1,len,0);//get hashkey 
    Hashtable_Insert_Node(hn,hv);//insert node to hashtable

    printf("--------key1=%s,key2=%s\n",key1,key2);
    strncpy(key1,key2,len);key1[len]='\0';
    strncpy(key2,keys+2*len,len);
    key2[len]=keys[n_keys-1];
    key2[len+1]='\0';

    hn=Init_Node(key1,key2);
    hv=hashkit_jenkins(key1,len,0);//get hashkey 
    Hashtable_Insert_Node(hn,hv);//insert node to hashtable

    printf("--------key1=%s,key2=%s\n",key1,key2);
    strncpy(key1,key2,len);key1[len]='\0';
    strncpy(key2,keys,len);
    key2[len]=keys[n_keys-1];
    key2[len+1]='\0';

    hn=Init_Node(key1,key2);
    hv=hashkit_jenkins(key1,len,0);//get hashkey 
    Hashtable_Insert_Node(hn,hv);//insert node to hashtable
    printf("--------key1=%s,key2=%s\n",key1,key2);
}

void  send_keys(char *keys)
{
   
    struct sockaddr_in servaddr;
    int sockfd;
    if((sockfd=socket(AF_INET,SOCK_STREAM,0))<=0)
    {
         printf("create socket error\n");
         return;
    }
    memset(&servaddr,0,sizeof(servaddr));
    servaddr.sin_family=AF_INET;
    servaddr.sin_port=htons(1840);
    if(inet_pton(AF_INET,"192.168.132.159",&servaddr.sin_addr)<=0)
    {
       printf("inet_pton error\n");
       return ;
     }
    if(connect(sockfd,(struct sockaddr*)&servaddr,sizeof(servaddr))<0)
    {
         printf("connect error\n");
         return ;
     }
    printf("keys==%s\n",keys);
    send(sockfd,keys,strlen(keys),0);
    close(sockfd);
}

//recv other client send keys
void* recv_keys(void*)
{
    int listenfd,connfd;
    struct sockaddr_in servaddr;
    if((listenfd=socket(AF_INET,SOCK_STREAM,0))==-1)
    {
       printf("socket create error\n");
       return NULL;
     }
     memset(&servaddr,0,sizeof(servaddr));
     servaddr.sin_family=AF_INET;
     servaddr.sin_addr.s_addr=htonl(INADDR_ANY);
     servaddr.sin_port=htons(1841);
     if(bind(listenfd,(struct sockaddr*)&servaddr,sizeof(servaddr))==-1)
     {
       printf("bind sock error\n");
       return NULL;
     }
     if(listen(listenfd,10)==-1)
     {
          printf("listen error\n");
          return NULL;
      }
       int n_keys=0;
       char keys_buff[1025];
       while(1)
       {
            if((connfd=accept(listenfd,(struct sockaddr*)NULL,NULL))==-1)
             {
	         printf("accept error\n");continue;
	     }
            n_keys=recv(connfd,keys_buff,1025,0);
            keys_buff[n_keys]='\0';
            store_keys(keys_buff,n_keys);
       } 
    close(listenfd);
}


