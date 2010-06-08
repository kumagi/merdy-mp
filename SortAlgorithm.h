#ifndef SORT_ARGORITHM_H_
#define SORT_ARGORITHM_H_

#include <algorithm>
#include <vector>
#include <iostream>

/*
	// �T���v���p�f�[�^�̏���
    const int num = 10;

    std::vector<int> target(num);    // vector�̏ꍇ

    std::vector<int> tidx(num);
    for (int i = 0; i < num; i++) {
        target[i] = rand();
        tidx[i] = i;
    }

    // vector�̏ꍇ
    std::sort(tidx.begin(), tidx.end(), IndexSortCmp(target.begin(), target.end() ) );

    // �\�[�g����
    std::cout << "sorted target index and target value\n";
    for (unsigned int i = 0; i < tidx.size()-1; i++) {
        std::cout << tidx[i] << ",";
    }
*/

/** �C���f�b�N�X�\�[�g�����֐��I�u�W�F�N�g
 * �O�����
 *   �V�[�P���X�̓����_���A�N�Z�X�����q�ɂ��A�N�Z�X�\
 */
template<class Ran>
class IndexSortFunctor {
public:
    /** �R���X�g���N�^
     *
     * @param  iter_begin_  �V�[�P���X�擪�̃����_���A�N�Z�X�����q
     * @param  iter_end_    �V�[�P���X�I�[�̎��̃����_���A�N�Z�X�����q
     */
    explicit IndexSortFunctor(const Ran iter_begin_, const Ran iter_end_) : itbg(iter_begin_), ited(iter_end_) {}

    /** �f�X�g���N�^
     */
    ~IndexSortFunctor() {}

    /** �֐��Ăяo��
     * �p�����[�^�́A[]���Z�q�̈�����z��
     * �K�v�������ited���g���Ĕ͈̓`�F�b�N���s�����Ƃ��\
     * �i�����ł͖������j
     */
    bool operator()(const size_t a, const size_t b) const {
        return *(itbg + a) > *(itbg + b);
    }

protected:
private:
    /** �C���f�b�N�X�\�[�g�Ώۂ̃R���e�i
     */
    const Ran itbg;
    const Ran ited;
};

/** �C���f�b�N�X�\�[�g�p�֐�
 */
template<class Ran>
inline IndexSortFunctor<Ran> IndexSortCmp(const Ran iter_begin_, const Ran iter_end_)
{
    return IndexSortFunctor<Ran>(iter_begin_, iter_end_);
}

#endif
