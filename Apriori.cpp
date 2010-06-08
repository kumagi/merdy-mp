#include "Apriori.h"

//output
std::ostream& operator<<(std::ostream& os, AprioriAlgorithm& a)
{
	os << "<attribute name>\n";
	for(int i=0; i<a.AttributeNum_; ++i){
		os << a.AttributeName_[i] << " ";
	}
	os << "\n\n";
	os << "<data> \n";
	for(int i=0; i<a.TupleNum_; ++i){
		for(int j=0; j<a.AttributeNum_; ++j){
			os << a.Database_(i,j) << " ";
		}
		os << "\n";
	}
	os << "\n<min support>\n" << a.MinSupport_ << "\n";
	return os;
}

bool AprioriAlgorithm::run(){

	//cout << "TupleNum, AttributeNum = " << TupleNum_ << ", " << AttributeNum_ << endl;

	// initialize
	int c_col = 0;
	int c_row = 0;
	matrix<int> C;	// candidates of frequent itemsets
	int l_col = 0;
	int l_row = 1;
	L_.resize(1);	// frequent itemsets
	Sup_.resize(1);	// support
	
	//cout << "==========<1-terate>==========" << endl;

	/* 1 - frequent itemsets */
	for(int i=0; i<AttributeNum_; ++i){

		// calculate support
		stringstream ss;
		ss << RULE_GET_COUNT << " " << AttributeName_[i] << " > 0";
		//cout << ss.str().c_str() << endl;
		//int count = Database.count(ss.str()); // カウント ***
		int count = 0; //ここから5行は一時的なカウント kesu
		for(int j=0; j<TupleNum_; ++j){
			if(Database_(j,i) == 1){
				++count;
			}
		}

		// if support is greater than minsup -> frequent itemsets
		double sup = (double)count/TupleNum_; //一時的
		if(sup - MinSupport_  > -TOL){
			++l_col;
			L_[0].resize(l_col,l_row);
			L_[0](l_col-1, 0) = i;
			Sup_[0].resize(l_col);
			Sup_[0][l_col-1] = sup;
		}
		//cout << "sup(" << i << ") = " << (double)count/TupleNum_ << endl;
	}

	//cout << "-----L(1)-----\n" << L_(0) << endl;
	//cout<< "L size = " << l_col << " * " << l_row << endl;
	
	for(int k=2; l_col>1; ++k){

		//cout << "==========<" << k << "-iterate>==========" << endl;

		// C init
		c_col = 0;
		c_row = k;
		C.resize(c_col,c_row);

		/* k - candidates */	// k-候補アイテム集合から計算(support)
		for(int i=0; i<l_col; ++i){
			for(int j=i+1; j<l_col; ++j){
				for(int p=0; p<l_row; ++p){
					if(p != l_row-1){	//最後以外は一致していて
						if(L_[k-2](i,p)!=L_[k-2](j,p)) break;
					}else{				//最後だけ違う場合、候補となる
						++c_col;
						C.resize(c_col,c_row);
						for(int q=0; q<l_row; ++q) //まず、L(i,:)をコピー
							C(c_col-1, q) = L_[k-2](i,q);
						C(c_col-1, c_row-1) = L_[k-2](j,l_row-1); //そんで最後にL(j,:)の最後のアイテム

						//今追加した部分集合が全て存在するか確認
						int id_L = 0;
						ublas::vector<int> subsetC(c_row-1); // Cの部分集合
						for(int r=c_row-1; r>=0; --r){ 

							for(int q=0; q<c_row-1; ++q){ // r番目がない部分集合
								subsetC[q] = C(c_col-1, r<=q?q+1:q);
							}
							//cout << "subset " << subsetC << endl;

							for(; id_L<l_col; ++id_L){ // (k-1)-LにsubsetCがあるかどうか
								int q;
								for(q=0; q<l_row; ++q){if(subsetC[q] != L_[k-2](id_L,q)) break;}
								if(q==l_row){break;} //一致していたら次のsubset
							}
						}
						if(id_L==l_col){ // (k-1)-Lに存在しないsubsetがある
							//cout << "not find subset..." << subsetC << endl;
							--c_col;
							C.resize(c_col,c_row); //今追加したのを消す
						}
					}
				}
			}
		}

		//cout << "-----C("<< k <<")-----\n" << C << endl;
		//cout<< "C size = " << c_col << " * " << c_row << endl;

		// L init
		l_col = 0;
		l_row = c_row;
		L_.resize(k);
		L_[k-1].resize(l_col,l_row);
		Sup_.resize(k);

		/*  k - frequent itemsets */	// k-候補アイテム集合から計算(support)
		for(int j=0; j<c_col; ++j){ // 全C

			// calculate support
			stringstream ss;
			ss << RULE_GET_COUNT;
			for(int i=0; i<c_row; ++i){
				if(i!=0) ss << " &";
				ss << " " << AttributeName_[C(j,i)] << " > 0";
			}
			//cout << ss.str().c_str() << endl;
			//int count = Database.count(ss.str()); // カウント
			int count = TupleNum_; // カウントは最大から減らす
			for(int p=0; p<TupleNum_; ++p){ // 全タプル
				for(int i=0; i<c_row; ++i){ // Cの全要素
					if(Database_(p,C(j,i)) == 0){ //指定されたアイテムがないならカウントを減らす
						--count;
						break;
					}
				}
			}
			//cout << "sup(" << j << ") = " << (double)count/TupleNum_ << endl;
			
			//minsupより大きいなら頻出アイテム
			double sup = (double)count/TupleNum_;
			//cout<< sup<< endl;
			if(sup - MinSupport_ > -TOL){
				++l_col;
				L_[k-1].resize(l_col,l_row);
				for(int p=0; p<c_row; ++p){
					L_[k-1](l_col-1, p) = C(j,p);
				}
				Sup_[k-1].resize(l_col);
				Sup_[k-1][l_col-1] = sup;
			}
		}

		//cout << "-----L("<< k <<")-----\n" << L_[k-1] << endl;
		//cout<< "L size = " << l_col << " * " << l_row << endl;

		// 頻出アイテム集合がないなら戻す
		if(l_col==0){
			L_.pop_back();
			Sup_.pop_back();
		}
	}

	return true;
}
