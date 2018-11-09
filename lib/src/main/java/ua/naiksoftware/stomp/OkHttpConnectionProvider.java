package ua.naiksoftware.stomp;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;

import org.java_websocket.framing.CloseFrame;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import io.reactivex.CompletableEmitter;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

class OkHttpConnectionProvider extends AbstractConnectionProvider {

    public static final String TAG = "OkHttpConnProvider";

    private final String mUri;
    @NonNull
    private final Map<String, String> mConnectHttpHeaders;
    private final OkHttpClient mOkHttpClient;

    @Nullable
    private WebSocket openSocket;

    @Nullable
    private CountDownLatch latch;

    OkHttpConnectionProvider(String uri, @Nullable Map<String, String> connectHttpHeaders, OkHttpClient okHttpClient) {
        super();
        mUri = uri;
        mConnectHttpHeaders = connectHttpHeaders != null ? connectHttpHeaders : new HashMap<>();
        mOkHttpClient = okHttpClient;
    }

    @Override
    public void rawDisconnect() {
        if (openSocket != null) {
            openSocket.close(CloseFrame.ABNORMAL_CLOSE, "");
            try {
                if (latch != null)
                    latch.await();
            } catch(InterruptedException e){
                Log.e(TAG, "Thread interrupted while waiting for Websocket closing: ", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    void createWebSocketConnection() {
        Request.Builder requestBuilder = new Request.Builder()
                .url(mUri);

        addConnectionHeadersToBuilder(requestBuilder, mConnectHttpHeaders);

        final CountDownLatch l = new CountDownLatch(1);
        latch = l;
        openSocket = mOkHttpClient.newWebSocket(requestBuilder.build(),
                new WebSocketListener() {
                    @Override
                    public void onOpen(WebSocket webSocket, @NonNull Response response) {
                        LifecycleEvent openEvent = new LifecycleEvent(LifecycleEvent.Type.OPENED);

                        TreeMap<String, String> headersAsMap = headersAsMap(response);

                        openEvent.setHandshakeResponseHeaders(headersAsMap);
                        emitLifecycleEvent(openEvent);
                    }

                    @Override
                    public void onMessage(WebSocket webSocket, String text) {
                        if (text.equals("\n"))
                            Log.d(TAG, "RECEIVED HEARTBEAT");
                        else
                            emitMessage(text);
                    }

                    @Override
                    public void onMessage(WebSocket webSocket, @NonNull ByteString bytes) {
                        emitMessage(bytes.utf8());
                    }

                    @Override
                    public void onClosed(WebSocket webSocket, int code, String reason) {
                        openSocket = null;
                        latch = null;
                        emitLifecycleEvent(new LifecycleEvent(LifecycleEvent.Type.CLOSED));
                        l.countDown();
                    }

                    @Override
                    public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                        // in OkHttp, a Failure is equivalent to a JWS-Error *and* a JWS-Close
                        emitLifecycleEvent(new LifecycleEvent(LifecycleEvent.Type.ERROR, new Exception(t)));
                        openSocket = null;
                        emitLifecycleEvent(new LifecycleEvent(LifecycleEvent.Type.CLOSED));
                    }

                    @Override
                    public void onClosing(final WebSocket webSocket, final int code, final String reason) {
                        webSocket.close(code, reason);
                    }
                }
        );
    }

    @Override
    void rawSend(String stompMessage) {
        openSocket.send(stompMessage);
    }

    @Nullable
    @Override
    Object getSocket() {
        return openSocket;
    }

    @NonNull
    private TreeMap<String, String> headersAsMap(@NonNull Response response) {
        TreeMap<String, String> headersAsMap = new TreeMap<>();
        Headers headers = response.headers();
        for (String key : headers.names()) {
            headersAsMap.put(key, headers.get(key));
        }
        return headersAsMap;
    }

    private void addConnectionHeadersToBuilder(@NonNull Request.Builder requestBuilder, @NonNull Map<String, String> mConnectHttpHeaders) {
        for (Map.Entry<String, String> headerEntry : mConnectHttpHeaders.entrySet()) {
            requestBuilder.addHeader(headerEntry.getKey(), headerEntry.getValue());
        }
    }
}
