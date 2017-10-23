#include <string>
#include <string.h>
#include <vector>
#include "aeroservice.hpp"

class AeroPool
{
public:
	~AeroPool() {}

	static AeroPool* GetInstance() {
		return m_instance;
	}

	bool initConn();
	inline AeroService* GetAeroService() { return m_pAeroService; }

private:
	bool _InitAeroService(AeroService** p_service);

private:
	static AeroPool* 	m_instance;
	AeroService*		m_pAeroService;
};
