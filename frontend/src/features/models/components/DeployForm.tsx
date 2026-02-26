import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';

const schema = z.object({
  model_id: z.coerce.number().int().positive('Required'),
  model_version_id: z.coerce.number().int().positive('Required'),
  service: z.string().min(1, 'Required'),
});

type FormValues = z.output<typeof schema>;

interface Props {
  onSubmit: (values: FormValues) => void;
  onCancel: () => void;
  loading?: boolean;
}

const services = ['snorkel', 'occupancy', 'hvac', 'general'];

const inputClass = 'w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500';

export default function DeployForm({ onSubmit, onCancel, loading }: Props) {
  const { register, handleSubmit, formState: { errors } } = useForm({
    resolver: zodResolver(schema),
    defaultValues: { model_id: '', model_version_id: '', service: 'occupancy' },
  });

  return (
    <form onSubmit={handleSubmit(onSubmit as never)} className="space-y-4">
      <div className="grid grid-cols-3 gap-4">
        <Field label="Model ID" error={errors.model_id?.message as string}>
          <input type="number" {...register('model_id')} className={inputClass} />
        </Field>
        <Field label="Model Version ID" error={errors.model_version_id?.message as string}>
          <input type="number" {...register('model_version_id')} className={inputClass} />
        </Field>
        <Field label="Service" error={errors.service?.message as string}>
          <select {...register('service')} className={inputClass}>
            {services.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onCancel} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={loading} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
          {loading ? 'Deploying...' : 'Deploy'}
        </button>
      </div>
    </form>
  );
}

function Field({ label, error, children }: { label: string; error?: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>
      {children}
      {error && <p className="text-xs text-red-500 mt-1">{error}</p>}
    </div>
  );
}
