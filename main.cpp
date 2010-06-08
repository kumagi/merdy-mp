#include <string>
#include <iostream>
#include<iomanip> //マニピュレータ
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/io.hpp>
#include "Apriori.h"

using namespace std;
using namespace boost::numeric::ublas;
using namespace boost::numeric;

#define WIDTH_DISPLAY_SUPPORT	10 // サポートの表示幅(文字数)
#define WIDTH_DISPLAY_ITEM		10 // 各アイテムの表示幅(文字数)
#define HIGHT_DISPLAY_ITEM		15 // アイテム集合の表示数(上位何位)
#define RULE_GET_ATTRIBUTE	"SELECT itemname FROM items"	// 属性(返り値:vector<string>)

#include <time.h> // Databese作成用(一時的なもの)
#include "SortAlgorithm.h"


// ソート(ソート後のインデックスを返す)
std::vector<int> sort(const std::vector<double>& Support){

    const int num = Support.size();

	std::vector<int> tidx(num); // index of support
    for (int i = 0; i < num; i++) {tidx[i] = i;}

    // index sort
    std::sort(tidx.begin(), tidx.end(), IndexSortCmp(Support.begin(), Support.end()));

    // result
    //std::cout << "sorted target index and target value\n";
    //for (unsigned int i = 0; i < tidx.size()-1; i++) {
    //    std::cout << Support[tidx[i]] << ",";
    //}
	//cout << endl;

	return tidx;
}

// コンソールに結果表示
void display(const matrix<int>& Ans, const std::vector<double>& Support, const std::vector<string>& AttName){
	std::vector<int> idx = sort(Support); // get sorted index of support

	// 項目表示
	cout << setw(WIDTH_DISPLAY_SUPPORT) << "support" << "|" << setw(WIDTH_DISPLAY_ITEM * Ans.size2()) << "items" << endl;

	// 線表示
	for(int i=0; i<(WIDTH_DISPLAY_SUPPORT); ++i) cout << "-";
	cout << "+";
	for(int i=0; i<(WIDTH_DISPLAY_ITEM * Ans.size2()); ++i) cout << "-";
	cout << endl;

	// 各アイテム集合を表示
	for(int j=0; j<Ans.size1(); ++j){
		if(j==HIGHT_DISPLAY_ITEM){ //表示の上限に達した場合
			cout << "etc..." << endl; 
			break;
		}else{
			cout << setw(WIDTH_DISPLAY_SUPPORT) << Support[idx[j]] << "|";
			for(int p=0; p<Ans.size2(); ++p){
				cout << setw(WIDTH_DISPLAY_ITEM) << AttName[Ans(idx[j],p)];
			}
			cout << endl;
		}
	}
}
/*
int main (int argc, char **argv){

	//========================================================
	//input
	
	// Databese作成用 (一時的)
	int tupleN = 100;
	int attriN = 10;
	int appearance_ratio = 60; // %
	
	// hyper parameter
	double min_sup = 0.375; // min support
	int K = 2;				// display k-frequent itemsets

	// set Database
	srand((unsigned) time(NULL));
	matrix<int> Database(tupleN,attriN); // database
	for(int i=0; i<tupleN; ++i){
		for(int j=0; j<attriN; ++j){
			Database(i,j) = (rand()%100) < appearance_ratio ? 1 : 0;
		}
	}
	
	for(;;){

		// set attribute name
		string aname_rule = RULE_GET_ATTRIBUTE;
		cout << aname_rule.c_str() << endl;
		// std::vector<string> AttName = Database.select(RULE_GET_ATTRIBUTE); //属性の名前
		std::vector<string> AttName(attriN); //とりあえず適当な名前5個分
		AttName[0] = "juice";
		AttName[1] = "cola";
		AttName[2] = "beer";
		AttName[3] = "wine";
		AttName[4] = "gin";
		AttName[5] = "kuma";
		AttName[6] = "uma";
		AttName[7] = "oden";
		AttName[8] = "cookie";
		AttName[9] = "been";

		// set apriori algorithm
		AprioriAlgorithm Apriori(AttName, Database, min_sup);

		// run apriori algorithm
		Apriori.run();

		
		// get result
		std::vector<matrix<int> > Ans = Apriori.getAns();
		std::vector<std::vector<double> > Support = Apriori.getSupport();

		usleep(10);
		cout << "\x1b[2J\x1b[1;1H" << std::flush;
		if(Ans.size()>=K){

			// display result
			cout << K << "-頻出アイテム集合" << endl;
			display(Ans[K-1], Support[K-1], AttName);
			//for(int i=0; i<Ans.size(); ++i){ //全部表示してみた
				//cout << i+1 << "-頻出アイテム集合" << endl;
				//display(Ans[i], Support[i], AttName);
			//}

		}else{
			cout << K << "frequently item set" << endl;
			cout << "not found" << endl;
		}

		// break; ///////////////////// roop break!!!!
	}

	cout << "program end...";
	string input_wait;
	cin >> input_wait;
	return 0;
}

/*/
// アルゴリズム確認用
int main (int argc, char **argv){

	//========================================================
	//input

	// hyper parameter
	double min_sup = 0.375;

	//attribute name
	std::vector<string> AttName(5);
	AttName[0] = "juice";
	AttName[1] = "cola";
	AttName[2] = "beer";
	AttName[3] = "wine";
	AttName[4] = "gin";

	matrix<int> Database(8,5);
	Database(0,0) = 1; Database(0,1) = 1; Database(0,2) = 1; Database(0,3) = 0; Database(0,4) = 0;
	Database(1,0) = 1; Database(1,1) = 1; Database(1,2) = 0; Database(1,3) = 1; Database(1,4) = 0;
	Database(2,0) = 1; Database(2,1) = 0; Database(2,2) = 0; Database(2,3) = 0; Database(2,4) = 1;
	Database(3,0) = 0; Database(3,1) = 1; Database(3,2) = 1; Database(3,3) = 0; Database(3,4) = 0;
	Database(4,0) = 1; Database(4,1) = 1; Database(4,2) = 1; Database(4,3) = 1; Database(4,4) = 0;
	Database(5,0) = 0; Database(5,1) = 0; Database(5,2) = 0; Database(5,3) = 0; Database(5,4) = 1;
	Database(6,0) = 1; Database(6,1) = 1; Database(6,2) = 0; Database(6,3) = 1; Database(6,4) = 1;
	Database(7,0) = 0; Database(7,1) = 0; Database(7,2) = 0; Database(7,3) = 1; Database(7,4) = 0;

	//cout << AttName << endl;
	//cout << Database << endl;

	AprioriAlgorithm Apriori(AttName, Database, min_sup);
	cout << Apriori << endl;
	Apriori.run();

	std::vector<matrix<int> > Ans = Apriori.getAns();
	std::vector<std::vector<double> > Support = Apriori.getSupport();

	for(int i=0; i<Ans.size(); ++i){ //全部表示してみた
		cout << i+1 << "-頻出アイテム集合" << endl;
		display(Ans[i], Support[i], AttName);
	}

	//==========================================================
	//example answer with min_sup = 37.5

	//1 item set support
	//juice	62.5%
	//cola	62.5%
	//beer	37.5%
	//wine	50.0%
	//gin	37.5%

	//2 item set support
	//juice, cola	50.0%
	//juice, wine	37.5%
	//cola,	beer	37.5%
	//cola, wine	37.5%

	//3 item set support
	//juice, cola, wine	37.5%

	cout << "program end...";
	string input_wait;
	cin >> input_wait;
	return 0;
}
//*/
