import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';

const schema = z.object({
  name: z.string().min(1, 'Required'),
  description: z.string().optional(),
  service: z.string().min(1, 'Required'),
  dataset_type: z.string().min(1, 'Required'),
  format: z.string().optional(),
  storage_path: z.string().optional(),
});

type FormValues = z.output<typeof schema>;

interface Props {
  onSubmit: (values: FormValues) => void;
  onCancel: () => void;
  loading?: boolean;
}

const services = ['snorkel', 'occupancy', 'hvac', 'general'];
const datasetTypes = ['index', 'rule_features', 'features', 'labels', 'training', 'predictions', 'occupancy_timeseries'];

const inputClass = 'w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500';

export default function DatasetRegisterForm({ onSubmit, onCancel, loading }: Props) {
  const { register, handleSubmit, formState: { errors } } = useForm({
    resolver: zodResolver(schema),
    defaultValues: { name: '', description: '', service: 'snorkel', dataset_type: 'index', format: 'parquet', storage_path: '' },
  });

  return (
    <form onSubmit={handleSubmit(onSubmit as never)} className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <Field label="Name" error={errors.name?.message as string}>
          <input {...register('name')} className={inputClass} />
        </Field>
        <Field label="Service" error={errors.service?.message as string}>
          <select {...register('service')} className={inputClass}>
            {services.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Dataset Type" error={errors.dataset_type?.message as string}>
          <select {...register('dataset_type')} className={inputClass}>
            {datasetTypes.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Format" error={errors.format?.message as string}>
          <input {...register('format')} className={inputClass} />
        </Field>
      </div>
      <Field label="Storage Path" error={errors.storage_path?.message as string}>
        <input {...register('storage_path')} className={inputClass} placeholder="s3://bucket/path/file.parquet" />
      </Field>
      <Field label="Description" error={errors.description?.message as string}>
        <textarea {...register('description')} className={`${inputClass} min-h-[80px] resize-y`} />
      </Field>
      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onCancel} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={loading} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
          {loading ? 'Registering...' : 'Register'}
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
