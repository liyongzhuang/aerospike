#include "aeropool.h"

AeroPool* AeroPool::m_instance = new AeroPool;

bool AeroPool::_InitAeroService(AeroService** p_service)
{
	*p_service = NULL;

	std::string str_error;
	AeroService* p_service_tmp = new AeroService(3, 100, 3000, "10.16.39.116:3000");
	if (p_service_tmp->Connect(str_error)) {
		*p_service = p_service_tmp;
		printf("connect ok!\n");
	}
	else {
		printf("connect failed!\n");
		delete p_service_tmp;
		return false;
	}

	return true;
}

bool AeroPool::initConn()
{
	return _InitAeroService(&m_pAeroService);
}
