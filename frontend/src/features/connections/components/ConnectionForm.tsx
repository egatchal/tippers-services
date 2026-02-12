import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import type { DatabaseConnection } from '../types/connection';

const schema = z.object({
  name: z.string().min(1, 'Required'),
  connection_type: z.string().min(1, 'Required'),
  host: z.string().min(1, 'Required'),
  port: z.coerce.number().int().positive(),
  database: z.string().min(1, 'Required'),
  user: z.string().min(1, 'Required'),
  password: z.string().min(1, 'Required'),
});

type FormValues = z.output<typeof schema>;

interface Props {
  initial?: DatabaseConnection;
  onSubmit: (values: FormValues) => void;
  onCancel: () => void;
  loading?: boolean;
}

const connectionTypes = ['postgresql', 'mysql', 'sqlite', 'mssql'];

const inputClass = 'w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500';

export default function ConnectionForm({ initial, onSubmit, onCancel, loading }: Props) {
  const { register, handleSubmit, formState: { errors } } = useForm({
    resolver: zodResolver(schema),
    defaultValues: initial
      ? { ...initial, password: '', port: String(initial.port) }
      : { name: '', connection_type: 'postgresql', host: 'localhost', port: '5432', database: '', user: '', password: '' },
  });

  return (
    <form onSubmit={handleSubmit(onSubmit as never)} className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <Field label="Name" error={errors.name?.message as string}>
          <input {...register('name')} className={inputClass} />
        </Field>
        <Field label="Type" error={errors.connection_type?.message as string}>
          <select {...register('connection_type')} className={inputClass}>
            {connectionTypes.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Host" error={errors.host?.message as string}>
          <input {...register('host')} className={inputClass} />
        </Field>
        <Field label="Port" error={errors.port?.message as string}>
          <input type="number" {...register('port')} className={inputClass} />
        </Field>
        <Field label="Database" error={errors.database?.message as string}>
          <input {...register('database')} className={inputClass} />
        </Field>
        <Field label="User" error={errors.user?.message as string}>
          <input {...register('user')} className={inputClass} />
        </Field>
      </div>
      <Field label="Password" error={errors.password?.message as string}>
        <input type="password" {...register('password')} className={inputClass} placeholder={initial ? '(leave blank to keep current)' : ''} />
      </Field>
      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onCancel} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={loading} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
          {loading ? 'Saving...' : initial ? 'Update' : 'Create'}
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
