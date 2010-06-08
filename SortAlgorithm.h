#ifndef SORT_ARGORITHM_H_
#define SORT_ARGORITHM_H_

#include <algorithm>
#include <vector>
#include <iostream>

/*
	// サンプル用データの準備
    const int num = 10;

    std::vector<int> target(num);    // vectorの場合

    std::vector<int> tidx(num);
    for (int i = 0; i < num; i++) {
        target[i] = rand();
        tidx[i] = i;
    }

    // vectorの場合
    std::sort(tidx.begin(), tidx.end(), IndexSortCmp(target.begin(), target.end() ) );

    // ソート結果
    std::cout << "sorted target index and target value\n";
    for (unsigned int i = 0; i < tidx.size()-1; i++) {
        std::cout << tidx[i] << ",";
    }
*/

/** インデックスソート条件関数オブジェクト
 * 前提条件
 *   シーケンスはランダムアクセス反復子によりアクセス可能
 */
template<class Ran>
class IndexSortFunctor {
public:
    /** コンストラクタ
     *
     * @param  iter_begin_  シーケンス先頭のランダムアクセス反復子
     * @param  iter_end_    シーケンス終端の次のランダムアクセス反復子
     */
    explicit IndexSortFunctor(const Ran iter_begin_, const Ran iter_end_) : itbg(iter_begin_), ited(iter_end_) {}

    /** デストラクタ
     */
    ~IndexSortFunctor() {}

    /** 関数呼び出し
     * パラメータは、[]演算子の引数を想定
     * 必要があればitedを使って範囲チェックを行うことも可能
     * （ここでは未実装）
     */
    bool operator()(const size_t a, const size_t b) const {
        return *(itbg + a) > *(itbg + b);
    }

protected:
private:
    /** インデックスソート対象のコンテナ
     */
    const Ran itbg;
    const Ran ited;
};

/** インデックスソート用関数
 */
template<class Ran>
inline IndexSortFunctor<Ran> IndexSortCmp(const Ran iter_begin_, const Ran iter_end_)
{
    return IndexSortFunctor<Ran>(iter_begin_, iter_end_);
}

#endif
