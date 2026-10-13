import { useEffect, useState } from 'react';
import { io } from 'socket.io-client';

const API = (import.meta as any).env?.VITE_API_URL ?? 'http://localhost:3000';

type Activation = {
  _id: string; customerId: string; planId: string; status: string; simulateFailure: string;
  history: { eventType: string; at: string }[];
};
const COLORS: Record<string, string> = { PENDING: '#999', IN_PROGRESS: '#e0a100', ACTIVE: '#1a9c3c', FAILED: '#d33' };
const BAD = ['BillingFailed', 'ProvisioningFailed', 'ActivationFailed', 'BillingAccountCancelled'];

export default function App() {
  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState('Plan Digital');
  const [simulateFailure, setFail] = useState('none');
  const [items, setItems] = useState<Record<string, Activation>>({});
  const [error, setError] = useState('');

  useEffect(() => {
    const socket = io(API); // RF-08: timeline en tiempo real por WebSocket
    socket.on('activation.update', (a: Activation) => setItems((p) => ({ ...p, [a._id]: a })));
    return () => { socket.close(); };
  }, []);

  async function submit(ev: React.FormEvent) {
    ev.preventDefault(); setError('');
    try {
      const r = await fetch(`${API}/activations`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, planId, simulateFailure }) });
      if (r.status !== 202) throw new Error(`HTTP ${r.status}`);
    } catch (e: any) { setError(e.message); }
  }

  const list = Object.values(items).sort((a, b) => b.history[0].at.localeCompare(a.history[0].at));
  return (
    <div style={{ fontFamily: 'system-ui', maxWidth: 640, margin: '1.5rem auto', padding: '0 1rem' }}>
      <h2>Contratar plan</h2>
      <form onSubmit={submit} style={{ display: 'grid', gap: 8 }}>
        <label>Cliente <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required /></label>
        <label>Plan <select value={planId} onChange={(e) => setPlanId(e.target.value)}>
          <option>Plan Digital</option><option>Plan Premium</option><option>Plan Familiar</option></select></label>
        <label>Forzar fallo en <select value={simulateFailure} onChange={(e) => setFail(e.target.value)}>
          <option value="none">ninguno</option><option value="billing">billing</option>
          <option value="provisioning">provisioning</option></select></label>
        <button type="submit">Contratar</button>
        {error && <span style={{ color: '#d33' }}>Error: {error}</span>}
      </form>

      <h3>Timeline de eventos</h3>
      {list.length === 0 && <p>Sin activaciones todavía.</p>}
      {list.map((a) => (
        <div key={a._id} style={{ border: '1px solid #ddd', borderRadius: 8, padding: 12, marginBottom: 12 }}>
          <b>{a._id}</b> · {a.customerId} · {a.planId}
          {a.simulateFailure !== 'none' && <i> (fallo forzado: {a.simulateFailure})</i>}
          <span style={{ float: 'right', color: '#fff', background: COLORS[a.status], borderRadius: 4, padding: '0 8px' }}>
            {a.status}</span>
          <ol style={{ margin: '8px 0 0', paddingLeft: 20 }}>
            {a.history.map((h, i) => (
              <li key={i} style={{ color: BAD.includes(h.eventType) ? '#d33' : '#1a9c3c' }}>
                {h.eventType} <small style={{ color: '#888' }}>{new Date(h.at).toLocaleTimeString()}</small></li>
            ))}
          </ol>
        </div>
      ))}
    </div>
  );
}
