/* Select the prebuilt offer for the active release; retain a verified fallback. */
fetch('/api/v1/meta', { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(15000) })
  .then(response => response.ok ? response.json() : null)
  .then(result => {
    const id = result?.release_id;
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id ?? '')) return;
    const base = `https://github.com/MisterPasha/UrbanLens/releases/download/data-${id}/urbanlens-${id}.zip`;
    document.getElementById('download').href = base;
    document.getElementById('checksum').href = `${base}.sha256`;
    document.getElementById('version').textContent = `Download for active release: ${id}.`;
  })
  .catch(() => { /* Keep the verified release when the API is unavailable. */ });
