import { useState } from 'react';
import { X, Play, Loader2 } from 'lucide-react';
import toast from 'react-hot-toast';
import type { Deployment, PredictResponse } from '../types/deployment';
import { predict } from '../api/serving';
import CodeEditor from '../../../shared/components/CodeEditor';

interface Props {
  deployment: Deployment;
  onClose: () => void;
}

const EXAMPLE_INPUT = JSON.stringify(
  { instances: [{ feature1: 1.0, feature2: 'value' }] },
  null,
  2,
);

export default function PredictModal({ deployment, onClose }: Props) {
  const [input, setInput] = useState(EXAMPLE_INPUT);
  const [result, setResult] = useState<PredictResponse | null>(null);
  const [loading, setLoading] = useState(false);

  const handlePredict = async () => {
    try {
      const parsed = JSON.parse(input);
      if (!parsed.instances || !Array.isArray(parsed.instances)) {
        toast.error('Input must have an "instances" array');
        return;
      }
      setLoading(true);
      const res = await predict(deployment.deployment_id, parsed);
      setResult(res);
    } catch (err) {
      if (err instanceof SyntaxError) {
        toast.error('Invalid JSON');
      } else {
        toast.error('Prediction failed');
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-lg shadow-xl w-[700px] max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div>
            <h3 className="text-lg font-semibold">Test Prediction</h3>
            <p className="text-xs text-gray-500">
              Deployment {deployment.deployment_id} &middot; {deployment.mlflow_model_uri}
            </p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded hover:bg-gray-100">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        <div className="flex-1 overflow-auto p-6 space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Input (JSON)</label>
            <CodeEditor value={input} onChange={(e) => setInput(e.target.value)} />
          </div>

          <button
            onClick={handlePredict}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            {loading ? 'Running...' : 'Run Prediction'}
          </button>

          {result && (
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Response</label>
              <pre className="p-3 bg-gray-50 border border-gray-200 rounded-md text-xs font-mono text-gray-800 overflow-x-auto max-h-[300px] overflow-y-auto">
                {JSON.stringify(result, null, 2)}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
