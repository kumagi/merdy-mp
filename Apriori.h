#ifndef APRIORIALGORITHM_H_
#define APRIORIALGORITHM_H_

#include <string>
#include <iostream>
#include <vector>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>

using namespace std;
using namespace boost::numeric::ublas;
using namespace boost::numeric;

#define TOL 1e-10
#define RULE_GET_COUNT		"COUNT FROM table WHERE"			// support計算(返り値:double)
#define RULE_GET_TUPLE_NUM	"COUNT FROM table WHERE *  site"	// タプル数(返り値:int)

class AprioriAlgorithm{

	//output(一時的)
	friend std::ostream& operator<< (std::ostream& os, AprioriAlgorithm &a);

private:
	std::vector<string> AttributeName_;		// attribute name
	matrix<int> Database_;					// database
	std::vector<matrix<int> > L_;			// frequent itemsets
	std::vector<std::vector<double> > Sup_;	// support
	int AttributeNum_, TupleNum_;			// database size
	double MinSupport_;						// min support

public:
	AprioriAlgorithm(std::vector<string>& AttributeName, matrix<int>& Database, double MinSupport)
	:AttributeName_(AttributeName), Database_(Database),
	AttributeNum_(Database_.size2()), TupleNum_(Database_.size1()), MinSupport_(MinSupport){
		string tpn_rule = RULE_GET_TUPLE_NUM;
		//cout << tpn_rule.c_str() << endl;
		//TupleNum_ = Database.count(RULE_GET_TUPLE_NUM); // タプル数
	}

	bool run(); // exe

	std::vector<matrix<int> > getAns(){return L_;}
	std::vector<std::vector<double> >getSupport(){return Sup_;}
};

#endif
