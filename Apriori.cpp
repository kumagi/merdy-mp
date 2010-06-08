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
		//int count = Database.count(ss.str()); // �J�E���g ***
		int count = 0; //��������5�s�͈ꎞ�I�ȃJ�E���g kesu
		for(int j=0; j<TupleNum_; ++j){
			if(Database_(j,i) == 1){
				++count;
			}
		}

		// if support is greater than minsup -> frequent itemsets
		double sup = (double)count/TupleNum_; //�ꎞ�I
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

		/* k - candidates */	// k-���A�C�e���W������v�Z(support)
		for(int i=0; i<l_col; ++i){
			for(int j=i+1; j<l_col; ++j){
				for(int p=0; p<l_row; ++p){
					if(p != l_row-1){	//�Ō�ȊO�͈�v���Ă���
						if(L_[k-2](i,p)!=L_[k-2](j,p)) break;
					}else{				//�Ōゾ���Ⴄ�ꍇ�A���ƂȂ�
						++c_col;
						C.resize(c_col,c_row);
						for(int q=0; q<l_row; ++q) //�܂��AL(i,:)���R�s�[
							C(c_col-1, q) = L_[k-2](i,q);
						C(c_col-1, c_row-1) = L_[k-2](j,l_row-1); //����ōŌ��L(j,:)�̍Ō�̃A�C�e��

						//���ǉ����������W�����S�đ��݂��邩�m�F
						int id_L = 0;
						ublas::vector<int> subsetC(c_row-1); // C�̕����W��
						for(int r=c_row-1; r>=0; --r){ 

							for(int q=0; q<c_row-1; ++q){ // r�Ԗڂ��Ȃ������W��
								subsetC[q] = C(c_col-1, r<=q?q+1:q);
							}
							//cout << "subset " << subsetC << endl;

							for(; id_L<l_col; ++id_L){ // (k-1)-L��subsetC�����邩�ǂ���
								int q;
								for(q=0; q<l_row; ++q){if(subsetC[q] != L_[k-2](id_L,q)) break;}
								if(q==l_row){break;} //��v���Ă����玟��subset
							}
						}
						if(id_L==l_col){ // (k-1)-L�ɑ��݂��Ȃ�subset������
							//cout << "not find subset..." << subsetC << endl;
							--c_col;
							C.resize(c_col,c_row); //���ǉ������̂�����
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

		/*  k - frequent itemsets */	// k-���A�C�e���W������v�Z(support)
		for(int j=0; j<c_col; ++j){ // �SC

			// calculate support
			stringstream ss;
			ss << RULE_GET_COUNT;
			for(int i=0; i<c_row; ++i){
				if(i!=0) ss << " &";
				ss << " " << AttributeName_[C(j,i)] << " > 0";
			}
			//cout << ss.str().c_str() << endl;
			//int count = Database.count(ss.str()); // �J�E���g
			int count = TupleNum_; // �J�E���g�͍ő傩�猸�炷
			for(int p=0; p<TupleNum_; ++p){ // �S�^�v��
				for(int i=0; i<c_row; ++i){ // C�̑S�v�f
					if(Database_(p,C(j,i)) == 0){ //�w�肳�ꂽ�A�C�e�����Ȃ��Ȃ�J�E���g�����炷
						--count;
						break;
					}
				}
			}
			//cout << "sup(" << j << ") = " << (double)count/TupleNum_ << endl;
			
			//minsup���傫���Ȃ�p�o�A�C�e��
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

		// �p�o�A�C�e���W�����Ȃ��Ȃ�߂�
		if(l_col==0){
			L_.pop_back();
			Sup_.pop_back();
		}
	}

	return true;
}
