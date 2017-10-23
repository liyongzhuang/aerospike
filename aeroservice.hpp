#ifndef _AERO_SERVICE_H_
#define _AERO_SERVICE_H_

#include <string>
#include <vector>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_record_iterator.h>

// 视频ip库数据
#define NS_COMMON "asdb"
#define SET_TVIPLIB "tviplib"
#define BIN_GAPIP "gapip"
#define BIN_CHEATIP "cheatip"
#define BIN_CHEATIP2 "cheatip2"
#define BIN_CHEATIPRT "cheatiprt"
#define BIN_MZIPS "mzips"
#define SET_TV_SPAM_IP "tvspamiplib"
#define BIN_TV_SPAM_IP "spamip"

// 视频移动端用户信息数据
#define SET_MTVUSER "mtvuser"
#define BIN_CPM2 "cpm2"
#define BIN_CONFLICT "conflict"
#define BIN_BARTURN "barturn"
#define BIN_ABNORMAL_IMEI       "ban_imei"
#define BIN_ABNORMAL_MAC        "ban_mac"
#define BIN_ABNORMAL_IDFA       "ban_idfa"
#define BIN_ABNORMAL_ANDROIDID  "ban_androidid"
#define BIN_NEWS_USERS_TIME     "news_users"

#define SET_DMP3RD   "dmp"
#define SET_USERPULSE   "userpulse"

// ctr组app文章页中插标签
#define NS_CTR       "asdb"
#define SET_CTR      "ctr_appnewstag_v1_1"
#define BIN_CTR      "appnewstag"

class AeroService {
public:
    AeroService(int connnum, int timeout, int port, const std::string &hosts) {
        // 初始化信息
        as_config t_config;
        as_config_init(&t_config);
        as_config_add_hosts(&t_config, hosts.c_str(), port);
        t_config.max_conns_per_node = connnum;
        t_config.async_max_conns_per_node = 0;
        t_config.pipe_max_conns_per_node = 0;
        t_config.conn_timeout_ms = timeout;
        t_config.policies.read.timeout = timeout;
        t_config.tender_interval = 1000;
        t_config.thread_pool_size = 0;
        aerospike_init(&m_asContext, &t_config);
        m_strHosts = hosts;        
    };

    ~AeroService() {
        if (m_asContext.cluster != NULL) {
            aerospike_close(&m_asContext, &m_asError);
        }
        aerospike_destroy(&m_asContext);
    };

    bool Connect(std::string &str_reason) {
        if (aerospike_connect(&m_asContext, &m_asError) != AEROSPIKE_OK) {
            str_reason = m_asError.message;
            return false;
        }
        return true;
    };

    bool Close(std::string &str_reason) {
        if (aerospike_close(&m_asContext, &m_asError) != AEROSPIKE_OK) {
            str_reason = m_asError.message;
            return false;
        }
        return true;
    }

     bool GetValMultiSet(const std::string& str_namespace, 
                        const std::string &str_set, 
                        const std::string str_key, 
                        std::string &str_result, 
                        std::string &str_reason) {
        if (!m_asContext.cluster)
            return false;
		
		if (str_key.empty())
            return false;

        as_key askey;
        as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
        as_record* p_rec = NULL;
        if (aerospike_key_get(&m_asContext, &m_asError, NULL, &askey, &p_rec) != AEROSPIKE_OK || !p_rec) {
            str_reason = m_asError.message;
            return false;
        }
        str_result.clear(); 
        _DecodeRecord(p_rec, str_result);
        as_record_destroy(p_rec);
        return true;
    }

    bool GetValBin(const std::string& str_namespace, 
                   const std::string &str_set,
                   const std::string &str_bin, 
                   const std::string str_key, 
                   std::string &str_result, 
                   std::string &str_reason) {
        if (!m_asContext.cluster)
            return false;

		if (str_key.empty())
            return false;

        as_key askey;
        as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
        as_record* p_rec = NULL;
        const char* bin[1] = {str_bin.c_str()};
        if (aerospike_key_select(&m_asContext, &m_asError, NULL, &askey, bin, &p_rec) != AEROSPIKE_OK || !p_rec) {
            str_reason = m_asError.message;
            return false;
        }
        str_result.clear(); 
        _DecodeRecord(p_rec, str_result);
        as_record_destroy(p_rec);
        return true;
    }

    bool GetValMultiBin(const std::string& str_namespace, 
                   const std::string &str_set,
                   std::vector<std::string> &vec_bin, 
                   const std::string str_key, 
                   std::string &str_result, 
                   std::string &str_reason) {
        if (!m_asContext.cluster)
            return false;

        if (str_key.empty())
            return false;

        as_key askey;
        as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
        as_record* p_rec = NULL;
        // const char* bin[1] = {str_bin.c_str()};
        char* select[100]; // = {"bin1", "bin2", "bin3", NULL};
        for (int i = 0; i < vec_bin.size(); ++i)
        {
            select[i] = (char*)vec_bin[i].c_str();
        }
        select[vec_bin.size()] = NULL;
        if (aerospike_key_select(&m_asContext, &m_asError, NULL, &askey, (const char**)select, &p_rec) != AEROSPIKE_OK || !p_rec) {
            str_reason = m_asError.message;
            return false;
        } 
        str_result.clear();
        _DecodeRecord(p_rec, str_result);
        as_record_destroy(p_rec);
        return true;
    }

     bool PutValBin(const std::string& str_namespace, 
                    const std::string &str_set,
                    const std::string &str_bin, 
                    const std::string str_key,
                    std::string &str_val,
                    std::string &str_reason,
                    int n_ttl = 0) {
        if (!m_asContext.cluster)
            return false;

        as_key askey;
        as_key_init(&askey, str_namespace.c_str(), str_set.c_str(), str_key.c_str());
        as_record rec;
        as_record_inita(&rec, 1);
        as_record_set_str(&rec, str_bin.c_str(), str_val.c_str());
        rec.ttl = n_ttl;
        if (aerospike_key_put(&m_asContext, &m_asError, NULL, &askey, &rec) != AEROSPIKE_OK) {
            str_reason = m_asError.message;
            return false;
        }
        as_record_destroy(&rec);
        return true;
    }

    std::string _GetHosts() {
        return m_strHosts;
    }
    
private:
    void _DecodeRecord(const as_record* p_rec, std::string &str_result) {
        if (p_rec->key.valuep) {
            char* key_val_as_str = as_val_tostring(p_rec->key.valuep);
            free(key_val_as_str);
        }
        uint16_t num_bins = as_record_numbins(p_rec);
        as_record_iterator it;
        as_record_iterator_init(&it, p_rec);
        std::string str_val;
        while (as_record_iterator_has_next(&it)) {
            str_val.clear();
            if (_GetValByBin(as_record_iterator_next(&it), str_val)) {
                if (str_result.empty())
                    str_result = str_val;
                else
                    str_result += "," + str_val;
            }
        }
        as_record_iterator_destroy(&it);
    }

    bool _GetValByBin(const as_bin* p_bin, std::string &str_val) {
        if (!p_bin)
            return false;

        char* val_as_str = NULL;
        val_as_str = as_val_tostring(as_bin_get_value(p_bin));
        str_val = std::string(val_as_str);
        free(val_as_str);
        _StrTrim(str_val);
        return !str_val.empty();
    }

    void _StrTrim(std::string& str) {
        str.erase(0, str.find_first_not_of("\""));
        str.erase(str.find_last_not_of("\"") + 1);
        str.erase(0, str.find_first_not_of("\t"));
        str.erase(str.find_last_not_of("\t") + 1);
    }

private:
    aerospike       m_asContext;
    as_error        m_asError;
    std::string     m_strHosts;
};

#endif
