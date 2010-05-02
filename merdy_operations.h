
enum flag{
	TYPE_OTHER = -1,
	TYPE_INT = 0,
	TYPE_STR = 1,
};

namespace OP{
	enum merdy_operations{
		// master
		SEND_DY_LIST,
		SEND_HASHES,
		SEND_MER_LIST,
		CREATE_SCHEMA,
		DELETE_SCHEMA,
		ADD_ME_DY,
		ADD_ME_MER,
		
		// dynamo
		UPDATE_HASHES,
		OK_ADD_ME_DY,
		SET_DY, // search cordinate
		OK_SET_DY,
		PUT_DY, // store data without everything
		OK_PUT_DY,
		GET_DY,
		DEL_DY,
		SET_COORDINATE,
		
		// merdy
		OK_ADD_ME_MER,
		UPDATE_MER_LIST,
	};
}
