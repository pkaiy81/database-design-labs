package app.tx.lock;

/**
 * ロックの取得に失敗した場合にスローされる例外。
 * 
 * <p>
 * この例外は、以下の状況で発生します：
 * <ul>
 * <li>ロック取得のタイムアウト</li>
 * <li>デッドロックの検出</li>
 * <li>ロック待機中の割り込み</li>
 * </ul>
 * 
 * <p>
 * この例外が発生した場合、通常はトランザクションをロールバックして
 * 再試行する必要があります。
 * 
 * @see Lock
 * @see LockManager
 */
public class LockAbortException extends RuntimeException {

    /**
     * 指定されたメッセージで例外を作成します。
     * 
     * @param message 例外メッセージ
     */
    public LockAbortException(String message) {
        super(message);
    }

    /**
     * 指定されたメッセージと原因で例外を作成します。
     * 
     * @param message 例外メッセージ
     * @param cause   例外の原因
     */
    public LockAbortException(String message, Throwable cause) {
        super(message, cause);
    }
}
