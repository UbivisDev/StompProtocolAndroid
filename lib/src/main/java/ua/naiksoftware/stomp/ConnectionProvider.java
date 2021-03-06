package ua.naiksoftware.stomp;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;

/**
 * Created by naik on 05.05.16.
 */
public interface ConnectionProvider {

    /**
     * Beginning with STOMP 1.1, the CONNECT frame requires the 'host' header, this method
     * provides an easy access to the host of the provider
     * @return the host of the provider connection
     */
    String getHost();

    /**
     * Subscribe this for receive stomp messages
     */
    Observable<String> messages();

    /**
     * Sending stomp messages via you ConnectionProvider.
     * onError if not connected or error detected will be called, or onCompleted id sending started
     * TODO: send messages with ACK
     */
    Completable send(String stompMessage);

    /**
     * Subscribe this for receive #LifecycleEvent events
     */
    Observable<LifecycleEvent> lifecycle();

    /**
     * Disconnects from server. This is basically a Callable.
     * Automatically emits Lifecycle.CLOSE
     */
    Completable disconnect();

    Completable setHeartbeat(int ms);
}
