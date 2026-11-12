package id.panproxyscanner;

import android.app.Activity;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.ViewGroup;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;

/**
 * Panel Pan ProxyScanner dalam sebuah WebView.
 *
 * Alamat panel disimpan di SharedPreferences supaya tidak perlu diketik ulang: isi dengan
 * alamat LAN mesin yang menjalankan scanner, mis. http://192.168.1.10:8787 (jalankan
 * `panproxyscanner.exe serve`; halaman utama menampilkan daftar alamat LAN yang bisa dipakai).
 */
public class MainActivity extends Activity {

    private static final String DEFAULT_URL = "http://192.168.1.2:8787";

    private WebView web;
    private SharedPreferences prefs;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        prefs = getSharedPreferences("panproxyscanner", MODE_PRIVATE);

        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);

        LinearLayout bar = new LinearLayout(this);
        bar.setOrientation(LinearLayout.HORIZONTAL);

        final EditText urlField = new EditText(this);
        urlField.setSingleLine(true);
        urlField.setHint(DEFAULT_URL);
        urlField.setText(prefs.getString("url", DEFAULT_URL));

        Button openButton = new Button(this);
        openButton.setText("Buka");

        bar.addView(urlField, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));
        bar.addView(openButton);

        web = new WebView(this);
        WebSettings settings = web.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setLoadWithOverviewMode(true);
        settings.setUseWideViewPort(true);
        web.setWebViewClient(new WebViewClient());
        web.setWebChromeClient(new WebChromeClient());

        openButton.setOnClickListener(v -> {
            String url = urlField.getText().toString().trim();
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                url = "http://" + url;
                urlField.setText(url);
            }
            prefs.edit().putString("url", url).apply();
            web.loadUrl(url);
        });

        root.addView(bar);
        root.addView(web, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f));
        setContentView(root);

        web.loadUrl(urlField.getText().toString());
    }

    @Override
    public void onBackPressed() {
        if (web != null && web.canGoBack()) {
            web.goBack();
            return;
        }
        super.onBackPressed();
    }
}
