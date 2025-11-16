package app.tx.lock;

/**
 * ロックの種類を表す列挙型。
 * 
 * <p>データベースの並行制御において、以下の2種類のロックを使用します：
 * <ul>
 *   <li><b>SHARED (共有ロック)</b>: 読み取り専用のロック。複数のトランザクションが同時に取得可能。</li>
 *   <li><b>EXCLUSIVE (排他ロック)</b>: 書き込み用のロック。1つのトランザクションのみが取得可能。</li>
 * </ul>
 * 
 * <h3>ロックの互換性マトリクス</h3>
 * <pre>
 * 既存のロック | SHARED | EXCLUSIVE
 * ------------|--------|----------
 * SHARED      |   ✓    |    ✗
 * EXCLUSIVE   |   ✗    |    ✗
 * </pre>
 * 
 * @see Lock
 * @see LockTable
 * @see LockManager
 */
public enum LockType {
    /**
     * 共有ロック (Shared Lock / Read Lock)
     * 
     * <p>読み取り操作に使用するロック。複数のトランザクションが同時に
     * 同じブロックに対して共有ロックを取得できます。
     * 
     * <p><b>使用例:</b>
     * <ul>
     *   <li>SELECT 文での読み取り</li>
     *   <li>getInt(), getString() などの読み取りメソッド</li>
     * </ul>
     */
    SHARED,
    
    /**
     * 排他ロック (Exclusive Lock / Write Lock)
     * 
     * <p>書き込み操作に使用するロック。1つのトランザクションのみが
     * ブロックに対して排他ロックを取得できます。排他ロックが取得されている間、
     * 他のトランザクションは共有ロックも排他ロックも取得できません。
     * 
     * <p><b>使用例:</b>
     * <ul>
     *   <li>INSERT, UPDATE, DELETE 文</li>
     *   <li>setInt(), setString() などの書き込みメソッド</li>
     * </ul>
     */
    EXCLUSIVE;
    
    /**
     * 指定されたロックタイプとの互換性を判定します。
     * 
     * @param other 判定対象のロックタイプ
     * @return 互換性がある場合は true、ない場合は false
     */
    public boolean isCompatibleWith(LockType other) {
        if (this == SHARED && other == SHARED) {
            return true;  // 共有ロック同士は互換性あり
        }
        return false;  // それ以外はすべて互換性なし
    }
    
    /**
     * このロックタイプが共有ロックかどうかを判定します。
     * 
     * @return 共有ロックの場合は true
     */
    public boolean isShared() {
        return this == SHARED;
    }
    
    /**
     * このロックタイプが排他ロックかどうかを判定します。
     * 
     * @return 排他ロックの場合は true
     */
    public boolean isExclusive() {
        return this == EXCLUSIVE;
    }
}
