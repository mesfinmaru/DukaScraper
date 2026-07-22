import React, { useState, useEffect } from 'react';

export default function App() {
  // Form State
  const [targetUrl, setTargetUrl] = useState('');
  const [workerType, setWorkerType] = useState('surface');
  const [isCrawling, setIsCrawling] = useState(false);
  const [statusMessage, setStatusMessage] = useState(null);

  // Storage & Telemetry State
  const [bucketName, setBucketName] = useState('');
  const [files, setFiles] = useState([]);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [previewContent, setPreviewContent] = useState(null);
  const [activeTab, setActiveTab] = useState('files'); // 'files' | 'history'

  // Crawl Job History
  const [crawlJobs, setCrawlJobs] = useState([]);

  // ----------------------------------------------------
  // 1. FETCH MINIO BUCKET FILES (What Workers Saved)
  // ----------------------------------------------------
  const fetchCrawledFiles = async () => {
    setLoadingFiles(true);
    try {
      const response = await fetch('http://localhost:8000/api/files');
      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      
      const data = await response.json();
      setBucketName(data.bucket || 'duka-raw-data');
      setFiles(data.files || []);
    } catch (err) {
      console.warn("Could not reach /api/files, using sandbox mode fallback.");
      // Fallback sandbox simulation files for testing UI
      setBucketName('duka-raw-data-sandbox');
      setFiles([
        'crawls/example_com_raw_payload.json',
        'crawls/shop_test_item_surface.html',
        'crawls/darknet_simulated_market_data.json'
      ]);
    } finally {
      setLoadingFiles(false);
    }
  };

  useEffect(() => {
    fetchCrawledFiles();
    const interval = setInterval(fetchCrawledFiles, 5000); // Poll every 5s for new files
    return () => clearInterval(interval);
  }, []);

  // ----------------------------------------------------
  // 2. DISPATCH CRAWL REQUEST TO BACKEND KAFKA QUEUE
  // ----------------------------------------------------
  const handleStartCrawl = async (e) => {
    e.preventDefault();
    
    let rawInput = targetUrl.trim();
    if (!rawInput) return;

    // --- AUTOMATIC URL SANITIZATION & CLEANUP ---
    // 1. Extract standard HTTP/HTTPS URL if wrapped in Markdown like [link](http...)
    let cleanedUrl = rawInput;
    const httpMatch = rawInput.match(/https?:\/\/[^\s\)\\]+/);

    if (httpMatch) {
      cleanedUrl = httpMatch[0];
    }

    // 2. Strip brackets, parentheses, single/double quotes, and trailing garbage
    cleanedUrl = cleanedUrl.replace(/[\[\]\(\)'"]/g, '').trim();

    // 3. Ensure a valid scheme prefix exists
    if (!cleanedUrl.startsWith('http://') && !cleanedUrl.startsWith('https://')) {
      cleanedUrl = `https://${cleanedUrl}`;
    }

    setIsCrawling(true);
    setStatusMessage(null);

    const jobId = `job_${Date.now()}`;
    const newJob = {
      id: jobId,
      url: cleanedUrl,
      worker: workerType,
      timestamp: new Date().toLocaleTimeString(),
      status: 'In Queue (Kafka)'
    };

    setCrawlJobs(prev => [newJob, ...prev]);

    try {
      const response = await fetch('http://localhost:8000/crawl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          url: cleanedUrl,
          worker_type: workerType
        })
      });

      if (!response.ok) throw new Error(`Server returned status ${response.status}`);

      const result = await response.json();
      
      setStatusMessage({
        type: 'success',
        text: `🚀 Success: ${cleanedUrl} queued for ${workerType}-worker! Check raw storage files below.`
      });

      // Update local job status
      setCrawlJobs(prev => prev.map(j => j.id === jobId ? { ...j, status: 'Processing / Crawling' } : j));
      
      setTargetUrl('');
      // Wait 3 seconds and refresh files list
      setTimeout(fetchCrawledFiles, 3000);

    } catch (err) {
      console.error("Crawl error:", err);
      setStatusMessage({
        type: 'error',
        text: `⚠️ Could not reach backend (or Kafka is down). Sandbox simulation activated for ${cleanedUrl}.`
      });

      // Simulation mode fallback
      const simulatedFileName = `crawls/${cleanedUrl.replace(/[^a-zA-Z0-9]/g, '_')}_scraped.json`;
      setFiles(prev => [simulatedFileName, ...prev]);
      setCrawlJobs(prev => prev.map(j => j.id === jobId ? { ...j, status: 'Completed (Simulated)' } : j));
      setTargetUrl('');
    } finally {
      setIsCrawling(false);
    }
  };

  // ----------------------------------------------------
  // 3. DOWNLOAD SPECIFIC CRAWLED FILE FROM MINIO
  // ----------------------------------------------------
  const handleDownloadFile = (fileName) => {
    if (!bucketName || bucketName.includes('sandbox')) {
      // Sandbox fallback download
      const mockData = JSON.stringify({
        source_url: targetUrl || "https://example.com",
        scraped_at: new Date().toISOString(),
        content: "<html><body><h1>Sample Scraped Output</h1><p>DukaScraper surface worker successfully extracted page text.</p></body></html>",
        status: 200
      }, null, 2);

      const blob = new Blob([mockData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName.split('/').pop() || 'scraped_output.json';
      a.click();
      return;
    }

    // Live Download via FastAPI /api/download endpoint
    const downloadUrl = `http://localhost:8000/api/download/${bucketName}/${encodeURIComponent(fileName)}`;
    window.open(downloadUrl, '_blank');
  };

  // ----------------------------------------------------
  // 4. PREVIEW FILE CONTENT IN MODAL/PANEL
  // ----------------------------------------------------
  const handlePreviewFile = (fileName) => {
    setPreviewContent({
      fileName,
      data: JSON.stringify({
        scraped_file: fileName,
        bucket: bucketName,
        timestamp: new Date().toISOString(),
        payload_preview: `<html>\n  <head><title>Scraped Document Preview</title></head>\n  <body>\n    <h1>Harvested Data from worker</h1>\n    <p>URL Content extracted and indexed cleanly.</p>\n  </body>\n</html>`
      }, null, 2)
    });
  };

  return (
    <div style={{
      padding: '32px',
      fontFamily: 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      backgroundColor: '#f8fafc',
      minHeight: '100vh',
      color: '#0f172a'
    }}>
      {/* Header */}
      <header style={{ marginBottom: '28px' }}>
        <h1 style={{ margin: 0, fontSize: '28px', fontWeight: '800', color: '#0f172a' }}>
          🕵️‍♂️ DukaScraper Crawl & Download Engine
        </h1>
        <p style={{ color: '#64748b', marginTop: '6px', fontSize: '15px' }}>
          Test target URLs, dispatch scraping workers, inspect raw payloads, and download crawled results.
        </p>
      </header>

      {/* CRAWL TRIGGER BOX */}
      <section style={{
        background: '#ffffff',
        padding: '24px',
        borderRadius: '12px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
        border: '1px solid #e2e8f0',
        marginBottom: '28px'
      }}>
        <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '700', color: '#1e293b' }}>
          🔗 Submit URL to Crawl
        </h2>

        <form onSubmit={handleStartCrawl} style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
          <input
            type="text"
            required
            placeholder="Paste target URL here (e.g., quotes.toscrape.com or https://quotes.toscrape.com)"
            value={targetUrl}
            onChange={(e) => setTargetUrl(e.target.value)}
            style={{
              flex: '1 1 380px',
              padding: '12px 16px',
              borderRadius: '8px',
              border: '1px solid #cbd5e1',
              fontSize: '14px',
              outline: 'none'
            }}
          />

          <select
            value={workerType}
            onChange={(e) => setWorkerType(e.target.value)}
            style={{
              padding: '12px 16px',
              borderRadius: '8px',
              border: '1px solid #cbd5e1',
              fontSize: '14px',
              backgroundColor: '#fff',
              cursor: 'pointer'
            }}
          >
            <option value="surface">Surface Worker (AioKafka)</option>
            <option value="deep">Deep Worker (Tor / Proxy)</option>
          </select>

          <button
            type="submit"
            disabled={isCrawling}
            style={{
              background: isCrawling ? '#94a3b8' : '#2563eb',
              color: '#ffffff',
              border: 'none',
              padding: '12px 24px',
              borderRadius: '8px',
              fontWeight: '600',
              fontSize: '14px',
              cursor: isCrawling ? 'not-allowed' : 'pointer',
              transition: 'background 0.2s'
            }}
          >
            {isCrawling ? 'Dispatching...' : '🚀 Start Crawling'}
          </button>
        </form>

        {statusMessage && (
          <div style={{
            marginTop: '16px',
            padding: '12px 16px',
            borderRadius: '8px',
            fontSize: '14px',
            backgroundColor: statusMessage.type === 'success' ? '#f0fdf4' : '#fef2f2',
            color: statusMessage.type === 'success' ? '#15803d' : '#b91c1c',
            border: `1px solid ${statusMessage.type === 'success' ? '#bbf7d0' : '#fecaca'}`
          }}>
            {statusMessage.text}
          </div>
        )}
      </section>

      {/* STORAGE & DOWNLOAD INSPECTOR TABLE */}
      <section style={{
        background: '#ffffff',
        padding: '24px',
        borderRadius: '12px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
        border: '1px solid #e2e8f0'
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h2 style={{ margin: 0, fontSize: '18px', fontWeight: '700', color: '#0f172a' }}>
              📁 Crawled Files & Raw Data Output
            </h2>
            <p style={{ margin: '4px 0 0 0', fontSize: '13px', color: '#64748b' }}>
              MinIO Storage Bucket: <code style={{ color: '#2563eb', fontWeight: 'bold' }}>{bucketName || 'duka-raw-data'}</code>
            </p>
          </div>

          <div style={{ display: 'flex', gap: '8px' }}>
            <button
              onClick={fetchCrawledFiles}
              style={{
                background: '#f1f5f9',
                color: '#334155',
                border: '1px solid #cbd5e1',
                padding: '8px 14px',
                borderRadius: '6px',
                cursor: 'pointer',
                fontWeight: '600',
                fontSize: '13px'
              }}
            >
              🔄 Refresh Storage
            </button>
          </div>
        </div>

        {loadingFiles ? (
          <p style={{ color: '#64748b', fontSize: '14px' }}>Loading bucket files from MinIO...</p>
        ) : files.length === 0 ? (
          <div style={{ padding: '32px', textAlign: 'center', backgroundColor: '#f8fafc', borderRadius: '8px', color: '#64748b' }}>
            <p style={{ margin: 0, fontSize: '15px' }}>📂 No crawled files found in MinIO bucket yet.</p>
            <p style={{ margin: '6px 0 0 0', fontSize: '13px' }}>Submit a URL above to start generating crawled files!</p>
          </div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left', fontSize: '14px' }}>
              <thead>
                <tr style={{ borderBottom: '2px solid #e2e8f0', color: '#475569', fontWeight: '600' }}>
                  <th style={{ padding: '12px' }}>File Path / Name</th>
                  <th style={{ padding: '12px' }}>Storage Bucket</th>
                  <th style={{ padding: '12px', textAlign: 'right' }}>Actions</th>
                </tr>
              </thead>
              <tbody>
                {files.map((file, idx) => (
                  <tr key={idx} style={{
                    borderBottom: '1px solid #f1f5f9',
                    backgroundColor: idx % 2 === 0 ? '#ffffff' : '#f8fafc'
                  }}>
                    <td style={{ padding: '12px', fontFamily: 'monospace', fontSize: '13px', color: '#1e293b', fontWeight: '600' }}>
                      📄 {file}
                    </td>
                    <td style={{ padding: '12px', color: '#64748b', fontSize: '13px' }}>
                      {bucketName}
                    </td>
                    <td style={{ padding: '12px', textAlign: 'right', display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
                      <button
                        onClick={() => handlePreviewFile(file)}
                        style={{
                          background: '#f1f5f9',
                          color: '#0f172a',
                          border: '1px solid #cbd5e1',
                          padding: '6px 12px',
                          borderRadius: '6px',
                          fontSize: '12px',
                          fontWeight: '600',
                          cursor: 'pointer'
                        }}
                      >
                        👁️ View Content
                      </button>

                      <button
                        onClick={() => handleDownloadFile(file)}
                        style={{
                          background: '#059669',
                          color: '#ffffff',
                          border: 'none',
                          padding: '6px 14px',
                          borderRadius: '6px',
                          fontSize: '12px',
                          fontWeight: '600',
                          cursor: 'pointer'
                        }}
                      >
                        📥 Download Scraped File
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* CONTENT PREVIEW MODAL */}
      {previewContent && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(15, 23, 42, 0.6)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 1000,
          padding: '20px'
        }}>
          <div style={{
            background: '#ffffff',
            borderRadius: '12px',
            maxWidth: '700px',
            width: '100%',
            maxHeight: '80vh',
            display: 'flex',
            flexDirection: 'column',
            boxShadow: '0 20px 25px -5px rgba(0, 0, 0, 0.1)'
          }}>
            <div style={{ padding: '16px 20px', borderBottom: '1px solid #e2e8f0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ margin: 0, fontSize: '16px', fontWeight: '700' }}>
                🔍 Scraped File Preview: <span style={{ color: '#2563eb' }}>{previewContent.fileName}</span>
              </h3>
              <button
                onClick={() => setPreviewContent(null)}
                style={{ background: 'none', border: 'none', fontSize: '18px', cursor: 'pointer', color: '#64748b' }}
              >
                ✖
              </button>
            </div>

            <div style={{ padding: '20px', overflowY: 'auto', flex: 1, backgroundColor: '#0f172a', color: '#38bdf8' }}>
              <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '13px', whiteSpace: 'pre-wrap' }}>
                {previewContent.data}
              </pre>
            </div>

            <div style={{ padding: '16px 20px', borderTop: '1px solid #e2e8f0', display: 'flex', justifyContent: 'flex-end', gap: '10px' }}>
              <button
                onClick={() => setPreviewContent(null)}
                style={{ padding: '8px 16px', borderRadius: '6px', border: '1px solid #cbd5e1', background: '#fff', cursor: 'pointer' }}
              >
                Close
              </button>
              <button
                onClick={() => handleDownloadFile(previewContent.fileName)}
                style={{ padding: '8px 16px', borderRadius: '6px', border: 'none', background: '#059669', color: '#fff', fontWeight: '600', cursor: 'pointer' }}
              >
                📥 Download Scraped Output
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}