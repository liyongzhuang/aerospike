#include "aeropool.h"

int main()
{
	// Connect
	if (!AeroPool::GetInstance()->initConn()) {
		return 1;
    }

    std::string str_key = "test", str_val = "123", str_error;
    AeroService* p_aeroservice = AeroPool::GetInstance()->GetAeroService();
    if (!p_aeroservice || !p_aeroservice->PutValBin(NS_COMMON, SET_MTVUSER, BIN_NEWS_USERS_TIME, str_key, str_val, str_error, 1000)) {
         printf("aeroget put failed\n");
        return 1;
    }

	if (!p_aeroservice || !p_aeroservice->GetValBin(NS_COMMON, SET_MTVUSER, BIN_NEWS_USERS_TIME, "test", str_val, str_error)) {
    	printf("aeroget failed!\n");
        return 1;
    } else {
        printf("aeroget ok, val:%s\n", str_val.c_str());
    }

    return 0;
}
